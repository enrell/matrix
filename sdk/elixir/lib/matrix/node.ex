defmodule Matrix.Node do
  @moduledoc """
  Generic Matrix test node for Elixir (ML1 contract: docs/ML1-NODE.md).

  Built as an escript (`mix escript.build` → `mx-node`):
  `mx-node --matrix-sock <sock> --id <logical> [--event-log <path>]
  [--stream-log <path>] [--stream-slow-ms <n>]`.
  """

  alias Matrix.{CallCtx, Component}
  alias Matrix.Errors.BusinessError

  use Matrix.Handler

  defstruct [:id, :event_log, :stream_log, :stream_slow_ms]

  @impl true
  def on_event(topic, payload) do
    # Node config travels via persistent_term: the dispatcher, call
    # tasks and main are different processes (one node per VM).
    if log = :persistent_term.get({__MODULE__, :event_log}, nil) do
      File.write!(log, "#{topic}\t#{:erlang.iolist_to_binary(:json.encode(payload))}\n", [:append])
    end

    :ok
  rescue
    _ -> :ok
  end

  @impl true
  def on_stream(stream_id, seq, payload) do
    if ms = :persistent_term.get({__MODULE__, :stream_slow_ms}, 0), do: Process.sleep(ms)

    if log = :persistent_term.get({__MODULE__, :stream_log}, nil) do
      File.write!(log, "#{stream_id}\t#{seq}\t#{byte_size(payload)}\n", [:append])
    end

    :ok
  rescue
    _ -> :ok
  end

  @impl true
  def on_call(ctx, _ticket, _cap, input, _ref) do
    st = :persistent_term.get({__MODULE__, :state})
    input = if is_map(input), do: input, else: %{}

    with :ok <- maybe_sleep(input, ctx),
         :ok <- maybe_fail(input) do
      dispatch(ctx, st, input)
    end
  rescue
    e in BusinessError -> {:error, e.code, e.message}
    e in Matrix.Errors.DepError -> {:error, e.code, e.message}
    e in Matrix.Errors.ResError -> {:error, e.code, e.message}
    e in Matrix.Errors.SdkError -> {:error, e.code, e.message}
  end

  defp maybe_sleep(%{"sleep_ms" => ms}, ctx) when is_integer(ms) and ms > 0 do
    if abortable_sleep(ms, ctx), do: raise(%BusinessError{code: "cancelled", message: "aborted"}), else: :ok
  end

  defp maybe_sleep(_, _), do: :ok

  defp maybe_fail(%{"fail" => code}) when is_binary(code) and code != "" do
    raise %BusinessError{code: code, message: "remote #{code}"}
  end

  defp maybe_fail(_), do: :ok

  defp abortable_sleep(ms, ctx) do
    abortable_loop(ms, ctx)
  end

  defp abortable_loop(left, _ctx) when left <= 0, do: false

  defp abortable_loop(left, ctx) do
    if CallCtx.cancelled?(ctx) do
      true
    else
      Process.sleep(min(5, left))
      abortable_loop(left - 5, ctx)
    end
  end

  defp dispatch(_ctx, st, %{"amplify" => n}) when is_integer(n) do
    size = n |> max(0) |> min(1_048_576)
    {:ok, %{"blob" => String.duplicate("x", size), "via" => st.id}}
  end

  defp dispatch(ctx, st, %{"chain" => true} = input) do
    case CallCtx.dependencies(ctx) do
      [] ->
        {:error, "dependency-unavailable", "no binding"}

      [first | _] ->
        inner = if is_map(input["input"]), do: input["input"], else: %{}
        timeout = if is_integer(input["timeout_ms"]), do: max(input["timeout_ms"], 1), else: 5_000

        try do
          out = CallCtx.invoke_dependency(ctx, first.id, inner, timeout)
          {:ok, %{"chained" => out, "via" => st.id}}
        rescue
          e in Matrix.Errors.DepError -> {:error, e.code, e.message}
        end
    end
  end

  defp dispatch(ctx, st, %{"acquire" => acq}) when is_map(acq) do
    try do
      h = CallCtx.acquire_resource(ctx, to_string(acq["kind"] || ""), to_string(acq["label"] || ""), acq["interval_ms"])
      {:ok, %{"acquired" => %{"handle" => to_string(h)}, "via" => st.id}}
    rescue
      e in Matrix.Errors.ResError -> {:error, e.code, e.message}
    end
  end

  defp dispatch(ctx, st, %{"release" => rel}) do
    h =
      cond do
        is_integer(rel) and rel >= 0 -> rel
        is_binary(rel) ->
          case Integer.parse(rel) do
            {n, ""} when n >= 0 -> n
            _ -> raise %BusinessError{code: "invalid-message", message: "bad release"}
          end
        true -> raise %BusinessError{code: "invalid-message", message: "bad release"}
      end

    try do
      :ok = CallCtx.release_resource(ctx, h)
      {:ok, %{"released" => to_string(h), "via" => st.id}}
    rescue
      e in Matrix.Errors.ResError -> {:error, e.code, e.message}
    end
  end

  defp dispatch(ctx, st, %{"stream_send" => spec}) when is_map(spec) do
    stream_id = if is_binary(spec["stream_id"]), do: spec["stream_id"], else: "s-test"
    chunks = clamp_int(spec["chunks"], 0, 256)
    nbytes = clamp_int(spec["chunk_bytes"], 0, 4096)
    slp = clamp_int(spec["sleep_ms"], 0, 9_999_999)
    payload = String.duplicate("x", nbytes)

    result =
      Enum.reduce_while(0..(chunks - 1)//1, 0, fn seq, sent ->
        cond do
          CallCtx.cancelled?(ctx) -> {:halt, {:cancelled}}
          true ->
            try do
              :ok = CallCtx.send_stream(ctx, stream_id, seq, payload)
              if slp > 0 do
                if abortable_sleep(min(slp, 50), ctx), do: throw(:aborted), else: :ok
              end
              {:cont, sent + 1}
            rescue
              e in Matrix.Errors.SdkError -> {:halt, {:refused, e.message}}
            end
        end
      end)

    case result do
      {:cancelled} -> {:error, "cancelled", "aborted"}
      {:refused, why} -> {:error, "stream-refused", why}
      sent -> {:ok, %{"stream_sent" => sent, "via" => st.id}}
    end
  catch
    :aborted -> {:error, "cancelled", "aborted"}
  end

  defp dispatch(ctx, st, %{"chain_with_streams" => spec}) when is_map(spec) do
    chain_with_streams(ctx, st, spec)
  end

  defp dispatch(_ctx, st, input) do
    {:ok, %{"echo" => input, "via" => st.id}}
  end

  defp chain_with_streams(ctx, st, spec) do
    stream_id = if is_binary(spec["stream_id"]), do: spec["stream_id"], else: "s-bidi"
    chunks = clamp_int(spec["chunks"], 0, 32)
    nbytes = clamp_int(spec["chunk_bytes"], 0, 1024)
    interval = clamp_int(spec["interval_ms"] || 20, 0, 50)
    prime = clamp_int(spec["prime_ms"] || 50, 0, 1000)
    payload = String.duplicate("x", nbytes)
    me = self()

    streamer =
      spawn(fn ->
        if prime > 0, do: Process.sleep(prime)
        sent =
          Enum.reduce_while(0..(chunks - 1)//1, 0, fn seq, n ->
            try do
              :ok = CallCtx.send_stream(ctx, stream_id, seq, payload)
              if interval > 0, do: Process.sleep(interval)
              {:cont, n + 1}
            rescue
              _ -> {:halt, n}
            end
          end)
        send(me, {:matrix_streamed, sent})
      end)

    result =
      case CallCtx.dependencies(ctx) do
        [] ->
          receive do
            {:matrix_streamed, _} -> :ok
          end
          {:error, "dependency-unavailable", "no binding"}

        [first | _] ->
          inner = if is_map(spec["input"]), do: spec["input"], else: %{}
          timeout = if is_integer(spec["timeout_ms"]), do: max(spec["timeout_ms"], 1), else: 8_000
          _ = streamer

          try do
            out = CallCtx.invoke_dependency(ctx, first.id, inner, timeout)
            sent = receive do
              {:matrix_streamed, n} -> n
            end
            {:ok, %{"chained" => out, "via" => st.id, "stream_sent" => sent}}
          rescue
            e in Matrix.Errors.DepError ->
              receive do
                {:matrix_streamed, _} -> :ok
              end
              {:error, e.code, e.message}
          end
      end

    result
  end

  defp clamp_int(v, lo, hi) when is_integer(v), do: v |> max(lo) |> min(hi)
  defp clamp_int(_, lo, _hi), do: lo

  # -- entrypoint ----------------------------------------------------------

  def main(argv) do
    opts = parse_argv(argv, %{})

    unless opts[:sock] do
      IO.puts(:stderr, "usage: mx-node --matrix-sock <sock> [--id <logical>] ...")
      System.halt(2)
    end

    id = opts[:id] || "dep-node"
    :persistent_term.put({__MODULE__, :state}, %__MODULE__{
      id: id,
      event_log: opts[:event_log],
      stream_log: opts[:stream_log],
      stream_slow_ms: opts[:stream_slow_ms] || 0
    })
    :persistent_term.put({__MODULE__, :event_log}, opts[:event_log])
    :persistent_term.put({__MODULE__, :stream_log}, opts[:stream_log])
    :persistent_term.put({__MODULE__, :stream_slow_ms}, opts[:stream_slow_ms] || 0)

    case Component.connect(opts[:sock], id) do
      {:ok, reader} ->
        # Death of this process takes the session down (linked
        # reader): no orphan context on crash (epic supervision row).
        case Component.serve(reader, __MODULE__) do
          :dispose -> System.halt(0)
          :eof -> System.halt(0)
          {:error, reason} ->
            IO.puts(:stderr, "serve: #{inspect(reason)}")
            System.halt(1)
        end

      {:error, reason} ->
        IO.puts(:stderr, "connect: #{reason}")
        System.halt(2)
    end
  end

  defp parse_argv([], acc), do: acc
  defp parse_argv(["--matrix-sock", v | rest], acc), do: parse_argv(rest, Map.put(acc, :sock, v))
  defp parse_argv(["--id", v | rest], acc), do: parse_argv(rest, Map.put(acc, :id, v))
  defp parse_argv(["--event-log", v | rest], acc), do: parse_argv(rest, Map.put(acc, :event_log, v))
  defp parse_argv(["--stream-log", v | rest], acc), do: parse_argv(rest, Map.put(acc, :stream_log, v))
  defp parse_argv(["--stream-slow-ms", v | rest], acc) do
    {n, _} = Integer.parse(v)
    parse_argv(rest, Map.put(acc, :stream_slow_ms, n))
  rescue
    _ -> parse_argv(rest, acc)
  end
  defp parse_argv([_ | rest], acc), do: parse_argv(rest, acc)
end

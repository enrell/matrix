defmodule Matrix.CallCtx do
  @moduledoc """
  Per-call context: bound session, streams, dependencies.

  Cancellation is cooperative: the reader sends `{:matrix_cancel,
  ticket}` to the call task on `call.cancel`; `cancelled?/1` polls the
  mailbox without consuming unrelated messages. Dependency/resource
  waits poll in 50 ms slices so cancel/deadline preempt the wait;
  deadlines include transport slack and never retry.
  """

  alias Matrix.Errors.{DepError, ResError}

  @event_cap 64

  defstruct [:reader, :ticket, :bindings, :cancel_ref]

  @type t :: %__MODULE__{
          reader: pid(),
          ticket: String.t(),
          bindings: [%{id: String.t(), capability: String.t()}],
          cancel_ref: reference()
        }

  def event_cap, do: @event_cap

  @doc "Bound session id."
  def session_id(%__MODULE__{reader: r}), do: Matrix.Component.session_id(r)

  @doc "True when the call was cancelled (mailbox polled, others kept)."
  def cancelled?(%__MODULE__{ticket: t}) do
    receive do
      {:matrix_cancel, ^t} -> true
    after
      0 -> false
    end
  end

  @doc "Edge-queue drops (slow observers)."
  def event_dropped_count(%__MODULE__{reader: r}) do
    Matrix.Component.dropped_count(r)
  end

  @doc "Queued stream chunks (credit signal)."
  def pending_stream_count(%__MODULE__{reader: r}) do
    Matrix.Component.pending_streams(r)
  end

  @doc "Opaque handles of this activation."
  def dependencies(%__MODULE__{bindings: b}), do: b

  @doc """
  Sends one text chunk. Non-UTF-8 binaries are refused explicitly,
  never lossy-converted.
  """
  def send_stream(%__MODULE__{} = ctx, stream_id, seq, payload)
      when is_binary(stream_id) and stream_id != "" and is_integer(seq) and seq >= 0 do
    unless String.valid?(payload) do
      raise %Matrix.Errors.SdkError{
        code: "invalid-message",
        phase: "stream",
        message: "stream payloads are text; binary must be refused, never lossy-converted"
      }
    end

    Matrix.Component.send_envelope(ctx.reader, "stream.data", nil, %{
      "stream_id" => stream_id,
      "seq" => Integer.to_string(seq),
      "payload" => payload
    })
  end

  def send_stream(_ctx, _stream_id, _seq, _payload) do
    raise %Matrix.Errors.SdkError{
      code: "invalid-message",
      phase: "stream",
      message: "bad stream id/seq (seq must be a non-negative integer)"
    }
  end

  @doc """
  Invokes a dependency by opaque handle. Blocks until terminal,
  inheriting call cancellation. Without local negotiation refuses with
  unsupported-feature, wire untouched.
  """
  def invoke_dependency(%__MODULE__{} = ctx, binding, input, timeout_ms)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    unless Matrix.Component.has_feature?(ctx.reader, "dependency-calls/1") do
      raise %DepError{code: "unsupported-feature", message: "dependency calls not negotiated"}
    end

    rid = Matrix.Component.fresh("r-dep")
    :ok = Matrix.Component.register_dep_waiter(ctx.reader, rid, self())

    :ok =
      Matrix.Component.send_envelope(ctx.reader, "dependency.open", rid, %{
        "parent_ticket" => ctx.ticket,
        "binding_id" => binding,
        "timeout_ms" => timeout_ms,
        "input" => input || %{}
      })

    deadline = System.monotonic_time(:millisecond) + timeout_ms + 10_000
    wait_dep(ctx, rid, deadline)
  end

  def invoke_dependency(_ctx, _binding, _input, _timeout) do
    raise %DepError{code: "invalid-message", message: "timeout must be positive"}
  end

  defp wait_dep(ctx, rid, deadline) do
    receive do
      {:matrix_dep_result, ^rid, {:ok, output}} ->
        output

      {:matrix_dep_result, ^rid, {:error, code, message}} ->
        raise %DepError{code: code, message: message}
    after
      50 ->
        cond do
          cancelled?(ctx) ->
            Matrix.Component.unregister_dep_waiter(ctx.reader, rid)
            Matrix.Component.send_dep_cancel(ctx.reader, rid)
            # Drain a racing terminal without blocking (waiter is gone).
            receive do
              {:matrix_dep_result, ^rid, _} -> :ok
            after
              0 -> :ok
            end

            raise %DepError{code: "cancelled", message: "parent cancelled"}

          System.monotonic_time(:millisecond) >= deadline ->
            Matrix.Component.unregister_dep_waiter(ctx.reader, rid)
            Matrix.Component.send_dep_cancel(ctx.reader, rid)

            receive do
              {:matrix_dep_result, ^rid, _} -> :ok
            after
              0 -> :ok
            end

            raise %DepError{code: "outcome-unknown", message: "sdk wait timeout"}

          true ->
            wait_dep(ctx, rid, deadline)
        end
    end
  end

  @doc "Acquires an activation resource (cap/sub/timer/task)."
  def acquire_resource(%__MODULE__{} = ctx, kind, label, interval_ms \\ nil) do
    fields = %{"kind" => to_string(kind), "label" => to_string(label)}
    fields = if is_nil(interval_ms), do: fields, else: Map.put(fields, "interval_ms", interval_ms)
    extra = resource_roundtrip(ctx, "acquire", fields)

    case extra do
      %{"handle" => h} ->
        case Integer.parse(to_string(h)) do
          {n, ""} when n >= 0 -> n
          _ -> raise %ResError{code: "internal", message: "missing handle"}
        end

      _ ->
        raise %ResError{code: "internal", message: "missing handle"}
    end
  end

  @doc "Releases a handle from `acquire_resource`."
  def release_resource(%__MODULE__{} = ctx, handle) when is_integer(handle) and handle >= 0 do
    resource_roundtrip(ctx, "release", %{"handle" => Integer.to_string(handle)})
    :ok
  end

  defp resource_roundtrip(ctx, operation, fields) do
    rid = Matrix.Component.fresh("r-res")
    :ok = Matrix.Component.register_res_waiter(ctx.reader, rid, self())
    body = Map.put(fields, "operation_id", Matrix.Component.fresh("op-res"))
    :ok = Matrix.Component.send_envelope(ctx.reader, "resource.#{operation}", rid, body)
    deadline = System.monotonic_time(:millisecond) + 10_000
    wait_res(ctx, rid, deadline)
  end

  defp wait_res(ctx, rid, deadline) do
    receive do
      {:matrix_res_result, ^rid, {:ok, extra}} ->
        extra

      {:matrix_res_result, ^rid, {:error, code, message}} ->
        raise %ResError{code: code, message: message}
    after
      50 ->
        cond do
          cancelled?(ctx) ->
            Matrix.Component.unregister_res_waiter(ctx.reader, rid)
            raise %ResError{code: "cancelled", message: "parent cancelled"}

          System.monotonic_time(:millisecond) >= deadline ->
            Matrix.Component.unregister_res_waiter(ctx.reader, rid)
            raise %ResError{code: "outcome-unknown", message: "resource wait timeout"}

          true ->
            wait_res(ctx, rid, deadline)
        end
    end
  end
end

defmodule Matrix.Component do
  @moduledoc """
  Connected component: negotiated, activated session (ML1, stdlib only).

  The reader is a process linked to the caller: if the caller dies, the
  reader dies, the socket closes and the kernel reaps the session — a
  dead consumer never leaves its context behind (epic supervision row).
  Call handlers run as supervised tasks; a crashing call answers a
  business `internal` error, never kills the session. Events/streams go
  to a dispatcher process over a bounded queue (64, drop-oldest,
  counted): slow observers throttle via host credit instead of stalling
  the reader.
  """

  use GenServer

  alias Matrix.{CallCtx, Framing}
  alias Matrix.Errors.{BusinessError, SdkError}

  @dependency_calls_feature "dependency-calls/1"

  defstruct [
    :sock,
    :max_frame,
    :session_id,
    :instance_id,
    :generation,
    :features,
    :bindings,
    :handler,
    :serve_caller,
    :serve_ref,
    :sup,
    :dispatcher,
    :owner,
    calls: %{},
    dep_waiters: %{},
    res_waiters: %{},
    ev_queue: :queue.new(),
    ev_len: 0,
    ev_dropped: 0,
    seq: 1
  ]

  # -- client API ----------------------------------------------------------

  @doc "Connects, negotiates, registers `logical`, confirms activation."
  def connect(sock_path, logical) do
    token = System.get_env("MATRIX_LAUNCH_TOKEN", "")

    case Framing.connect(sock_path) do
      {:ok, sock} ->
        case handshake(sock, token, logical) do
          {:ok, reader} -> {:ok, reader}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, "connect: #{inspect(reason)}"}
    end
  end

  defp handshake(sock, token, logical) do
    with {:ok, ^sock} <- {:ok, sock},
         :ok <-
           Framing.send_frame(sock, %{
             "protocol" => "matrix.component",
             "version" => "0.1",
             "type" => "hello",
             "message_id" => "h1",
             "body" => %{
               "launch_token" => token,
               "versions" => ["0.1"],
               "max_frame" => Framing.max_frame(),
               "client" => "matrix-component-ex",
               "features" => [@dependency_calls_feature]
             }
           }),
         {:ok, welcome} <- Framing.read_frame(sock),
         "welcome" <- welcome["type"],
         session = welcome["session_id"],
         max_frame = get_in(welcome, ["body", "max_frame"]) || Framing.max_frame(),
         features = get_in(welcome, ["body", "features"]) || [],
         :ok <-
           Framing.send_frame(
             sock,
             %{
               "protocol" => "matrix.component",
               "version" => "0.1",
               "type" => "component.register",
               "message_id" => "reg1",
               "session_id" => session,
               "body" => %{"manifest" => %{"id" => logical}}
             },
             max_frame
           ),
         {:ok, reg} <- Framing.read_frame(sock, max_frame),
         "registered" <- reg["type"],
         {:ok, act} <- Framing.read_frame(sock, max_frame),
         "lifecycle.activate" <- act["type"] do
      bindings =
        (get_in(act, ["body", "dependency_bindings"]) || [])
        |> Enum.flat_map(fn
          %{"binding_id" => id, "capability" => cap} when id != "" and cap != "" ->
            [%{id: id, capability: cap}]

          _ ->
            []
        end)

      op = get_in(act, ["body", "operation_id"]) || "op?"

      :ok =
        Framing.send_frame(
          sock,
          %{
            "protocol" => "matrix.component",
            "version" => "0.1",
            "type" => "lifecycle.result",
            "message_id" => "lc1",
            "session_id" => session,
            "instance_id" => reg["instance_id"],
            "generation" => reg["generation"],
            "request_id" => act["request_id"],
            "body" => %{"operation_id" => op, "status" => "ok", "pending" => []}
          },
          max_frame
        )

      {:ok, sup} = Task.Supervisor.start_link()

      state = %__MODULE__{
        sock: sock,
        max_frame: max_frame,
        session_id: session,
        instance_id: reg["instance_id"],
        generation: to_string(reg["generation"]),
        features: Enum.filter(features, &is_binary/1),
        bindings: bindings
      }

      {:ok, reader} =
        GenServer.start_link(__MODULE__, {state, sup, self()})

      :ok = :gen_tcp.controlling_process(sock, reader)
      {:ok, reader}
    else
      {:error, reason} ->
        :gen_tcp.close(sock)
        {:error, "connect: #{inspect(reason)}"}

      other ->
        :gen_tcp.close(sock)
        {:error, "connect: unexpected #{inspect(other)}"}
    end
  end

  @doc """
  Serves until EOF/error, quiesce or dispose. Returns `:dispose`, `:eof`
  or `{:error, reason}`. Blocks the caller; the reader is linked, so a
  dying caller takes the session down (no orphan context).
  """
  def serve(reader, handler) do
    ref = Process.monitor(reader)
    GenServer.cast(reader, {:serve, handler, self(), ref})

    receive do
      {:matrix_served, ^ref, reason} -> reason
      {:DOWN, ^ref, :process, ^reader, _} -> :eof
    end
  end

  @doc false
  def session_id(reader), do: GenServer.call(reader, :session_id)

  @doc false
  def dropped_count(reader), do: GenServer.call(reader, :dropped_count)

  @doc false
  def pending_streams(reader), do: GenServer.call(reader, :pending_streams)

  @doc false
  def has_feature?(reader, f), do: GenServer.call(reader, {:has_feature, f})

  @doc false
  def fresh(prefix), do: "#{prefix}-#{:erlang.unique_integer([:positive, :monotonic])}"

  @doc false
  def register_dep_waiter(reader, rid, pid),
    do: GenServer.call(reader, {:register_dep, rid, pid})

  @doc false
  def unregister_dep_waiter(reader, rid),
    do: GenServer.cast(reader, {:unregister_dep, rid})

  @doc false
  def register_res_waiter(reader, rid, pid),
    do: GenServer.call(reader, {:register_res, rid, pid})

  @doc false
  def unregister_res_waiter(reader, rid),
    do: GenServer.cast(reader, {:unregister_res, rid})

  @doc false
  def send_envelope(reader, type, rid, body),
    do: GenServer.call(reader, {:send, type, rid, body})

  @doc false
  def send_dep_cancel(reader, rid),
    do: GenServer.cast(reader, {:send_dep_cancel, rid})

  # -- server --------------------------------------------------------------

  @impl true
  def init({state, sup, owner}) do
    Process.flag(:trap_exit, true)
    {:ok, %{state | sup: sup, owner: owner}}
  end

  @impl true
  def handle_call(:session_id, _from, state), do: {:reply, state.session_id, state}

  def handle_call(:dropped_count, _from, state), do: {:reply, state.ev_dropped, state}

  # Dispatcher pulls the current batch; the reader keeps owning the
  # bounded queue so overflow is counted at enqueue time.
  def handle_call(:take_batch, _from, state) do
    {:reply, :queue.to_list(state.ev_queue), %{state | ev_queue: :queue.new(), ev_len: 0}}
  end

  def handle_call(:pending_streams, _from, state) do
    n =
      :queue.to_list(state.ev_queue)
      |> Enum.count(fn
        {:stream, _, _, _} -> true
        _ -> false
      end)

    {:reply, n, state}
  end

  def handle_call({:has_feature, f}, _from, state),
    do: {:reply, f in state.features, state}

  def handle_call({:register_dep, rid, pid}, _from, state),
    do: {:reply, :ok, %{state | dep_waiters: Map.put(state.dep_waiters, rid, pid)}}

  def handle_call({:register_res, rid, pid}, _from, state),
    do: {:reply, :ok, %{state | res_waiters: Map.put(state.res_waiters, rid, pid)}}

  def handle_call({:send, type, rid, body}, _from, state) do
    reply = send_env(state, type, fresh("m"), rid, body)

    case reply do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:serve, handler, caller, ref}, state) do
    reader = self()

    disp =
      spawn_link(fn ->
        dispatcher_loop(%{queue: :queue.new(), len: 0, dropped: 0, handler: handler, reader: reader})
      end)

    # The pump starts here, after the handler is installed: no frame
    # is ever dispatched handler-less (pre-serve bytes wait in the
    # socket buffer under normal TCP backpressure).
    spawn_link(fn -> pump_loop(reader, state.sock, state.max_frame) end)

    {:noreply, %{state | handler: handler, serve_caller: caller, serve_ref: ref, dispatcher: disp}}
  end

  def handle_cast({:unregister_dep, rid}, state),
    do: {:noreply, %{state | dep_waiters: Map.delete(state.dep_waiters, rid)}}

  def handle_cast({:unregister_res, rid}, state),
    do: {:noreply, %{state | res_waiters: Map.delete(state.res_waiters, rid)}}

  def handle_cast({:send_dep_cancel, rid}, state) do
    send_env(state, "dependency.cancel", fresh("m-dep-cancel"), fresh("r-dep-cancel"), %{
      "target_request_id" => rid
    })

    {:noreply, state}
  end

  def handle_cast({:frame, :eof}, state), do: {:noreply, finish(state, :eof)}
  def handle_cast({:frame, :malformed}, state), do: {:noreply, state}
  def handle_cast({:frame, {:error, _}}, state), do: {:noreply, finish(state, :eof)}
  def handle_cast({:frame, {:ok, env}}, state), do: {:noreply, dispatch(state, env)}

  @impl true
  def handle_info({:EXIT, _pid, _reason}, state) do
    # Linked owner/task/dispatcher death: the session cannot outlive
    # its context. If the owner died, go down too (no orphan session).
    {:stop, :normal, state}
  end

  def handle_info({:matrix_call_done, _ref, _ticket}, state), do: {:noreply, state}
  def handle_info(_msg, state), do: {:noreply, state}

  # -- internals -----------------------------------------------------------

  defp pump_loop(reader, sock, max_frame) do
    case Framing.read_frame(sock, max_frame) do
      {:eof} ->
        GenServer.cast(reader, {:frame, :eof})

      {:malformed} ->
        GenServer.cast(reader, {:frame, :malformed})
        if Process.alive?(reader), do: pump_loop(reader, sock, max_frame)

      {:error, reason} ->
        GenServer.cast(reader, {:frame, {:error, reason}})

      {:ok, env} ->
        GenServer.cast(reader, {:frame, {:ok, env}})
        if Process.alive?(reader), do: pump_loop(reader, sock, max_frame)
    end
  end

  defp send_env(state, type, mid, rid, body) do
    msg = %{
      "protocol" => "matrix.component",
      "version" => "0.1",
      "type" => type,
      "message_id" => mid,
      "session_id" => state.session_id,
      "instance_id" => state.instance_id,
      "generation" => state.generation,
      "body" => body
    }

    msg = if rid, do: Map.put(msg, "request_id", rid), else: msg

    case Framing.send_frame(state.sock, msg, state.max_frame) do
      :ok -> :ok
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  defp bound_ok?(state, env) do
    env["session_id"] == state.session_id and
      (is_nil(env["instance_id"]) or env["instance_id"] == state.instance_id) and
      (is_nil(env["generation"]) or Framing.gen_equal?(env["generation"], state.generation))
  end

  defp dispatch(state, env) do
    if state.handler == nil or not bound_ok?(state, env) do
      state
    else
      body = env["body"] || %{}
      handle(state, env["type"], body, env["request_id"])
    end
  end

  defp handle(state, type, body, rid)
       when type in ["lifecycle.prepare", "lifecycle.activate", "lifecycle.quiesce"] do
    send_env(state, "lifecycle.result", "m-lc", rid, %{
      "operation_id" => body["operation_id"] || "op?",
      "status" => "ok",
      "pending" => []
    })

    state
  end

  defp handle(state, "lifecycle.dispose", body, rid) do
    send_env(state, "lifecycle.result", "m-lc", rid, %{
      "operation_id" => body["operation_id"] || "op?",
      "status" => "ok",
      "pending" => []
    })

    finish(state, :dispose)
  end

  defp handle(state, "call.open", body, rid) do
    ticket = body["ticket"] || ""
    cap = body["capability"] || ""
    input = Map.get(body, "input", %{})
    ctx_ref = make_ref()
    ctx = %CallCtx{reader: self(), ticket: ticket, bindings: state.bindings, cancel_ref: ctx_ref}
    handler = state.handler
    reader = self()

    {:ok, task_pid} =
      Task.Supervisor.start_child(state.sup, fn ->
        result =
          try do
            handler.on_call(ctx, ticket, cap, input, ctx_ref)
          rescue
            e in BusinessError -> {:error, e.code, e.message}
            e in SdkError -> {:error, e.code, e.message}
            e -> {:error, "internal", "handler: #{Exception.message(e)}"}
          end

        # Late after cancel: stay silent (no false success).
        unless cancelled_now?(ticket) do
          send_result(reader, ticket, rid, result)
        end
      end, restart: :temporary)

    %{state | calls: Map.put(state.calls, ticket, task_pid)}
  end

  defp handle(state, "call.cancel", body, _rid) do
    ticket = body["ticket"] || ""

    case Map.get(state.calls, ticket) do
      nil -> :ok
      pid -> send(pid, {:matrix_cancel, ticket})
    end

    spawn(fn ->
      try do
        state.handler.on_cancel(ticket)
      rescue
        _ -> :ok
      end
    end)

    state
  end

  defp handle(state, "dependency.result", body, rid) do
    case Map.pop(state.dep_waiters, rid) do
      {nil, _} ->
        state

      {pid, waiters} ->
        if body["status"] == "ok" do
          send(pid, {:matrix_dep_result, rid, {:ok, body["output"]}})
        else
          err = body["error"] || %{}
          send(
            pid,
            {:matrix_dep_result, rid,
             {:error, err["code"] || "internal", err["message"] || "remote error"}}
          )
        end

        %{state | dep_waiters: waiters}
    end
  end

  defp handle(state, "resource.result", body, rid) do
    case Map.pop(state.res_waiters, rid) do
      {nil, _} ->
        state

      {pid, waiters} ->
        if body["status"] == "ok" do
          extra = Map.drop(body, ["operation_id", "status"])
          send(pid, {:matrix_res_result, rid, {:ok, extra}})
        else
          send(
            pid,
            {:matrix_res_result, rid,
             {:error, body["code"] || "internal", body["message"] || "remote error"}}
          )
        end

        %{state | res_waiters: waiters}
    end
  end

  defp handle(state, "event.deliver", body, _rid) do
    if is_binary(body["topic"]) and body["topic"] != "" do
      enqueue(state, {:event, body["topic"], body["payload"]})
    else
      state
    end
  end

  defp handle(state, "stream.data", body, _rid) do
    with sid when is_binary(sid) <- body["stream_id"],
         seq when is_binary(seq) <- body["seq"],
         {n, ""} when n >= 0 <- Integer.parse(seq),
         payload when is_binary(payload) <- body["payload"] do
      enqueue(state, {:stream, sid, n, payload})
    else
      _ -> state
    end
  end

  # Other types: ignored without dropping the session.
  defp handle(state, _type, _body, _rid), do: state

  defp enqueue(state, item) do
    {q, len, dropped} =
      if state.ev_len >= CallCtx.event_cap() do
        {_, q} = :queue.out(state.ev_queue)
        {:queue.in(item, q), state.ev_len, state.ev_dropped + 1}
      else
        {:queue.in(item, state.ev_queue), state.ev_len + 1, state.ev_dropped}
      end

    if state.dispatcher, do: send(state.dispatcher, :wake)
    %{state | ev_queue: q, ev_len: len, ev_dropped: dropped}
  end

  defp cancelled_now?(ticket) do
    receive do
      {:matrix_cancel, ^ticket} -> true
    after
      0 -> false
    end
  end

  defp send_result(reader, ticket, rid, result) do
    rbody =
      case result do
        {:ok, out} -> %{"ticket" => ticket, "status" => "ok", "output" => out}
        {:error, code, message} -> %{"ticket" => ticket, "status" => "error", "error" => %{"code" => code, "message" => message}}
        out -> %{"ticket" => ticket, "status" => "ok", "output" => out}
      end

    GenServer.call(reader, {:send, "call.result", rid, rbody})
  end

  defp finish(state, reason) do
    if state.serve_caller do
      send(state.serve_caller, {:matrix_served, state.serve_ref, reason})
    end

    # Withdrawal revokes everything: wake waiters with their real
    # request ids (they raise outcome-unknown immediately instead of
    # sleeping to deadline), forget calls. Late terminals find no
    # waiter and stay silent.
    Enum.each(state.dep_waiters, fn {rid, pid} ->
      send(pid, {:matrix_dep_result, rid, {:error, "outcome-unknown", "session ended"}})
    end)

    Enum.each(state.res_waiters, fn {rid, pid} ->
      send(pid, {:matrix_res_result, rid, {:error, "outcome-unknown", "session ended"}})
    end)

    try do
      :gen_tcp.close(state.sock)
    rescue
      _ -> :ok
    end

    %{state | dep_waiters: %{}, res_waiters: %{}, calls: %{}, serve_caller: nil}
  end

  # Dispatcher: pulls batches from the reader-owned bounded queue and
  # delivers them off the read path. Slow observers throttle via host
  # credit instead of stalling calls; overflow was already counted at
  # enqueue time. Coalesced wakes are harmless (empty take = no-op).
  # Takes coalesce 50ms after idle: a take is a microsecond round trip
  # (unlike other SDKs' slow dispatcher pops), so without coalescing a
  # burst could be drained before it accumulates and overflow would
  # never trigger. The sleep only delays the first batch after idle;
  # backlogged takes drain immediately.
  defp dispatcher_loop(%{handler: handler, reader: reader} = st) do
    receive do
      :wake ->
        Process.sleep(50)
        drain_all(handler, reader)
        dispatcher_loop(st)
    end
  end

  defp drain_all(handler, reader) do
    case GenServer.call(reader, :take_batch, 5_000) do
      [] -> :ok
      batch ->
        Enum.each(batch, &deliver(handler, &1))
        drain_all(handler, reader)
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp deliver(handler, {:event, topic, payload}) do
    try do
      handler.on_event(topic, payload)
    rescue
      _ -> :ok
    end
  end

  defp deliver(handler, {:stream, sid, seq, payload}) do
    try do
      handler.on_stream(sid, seq, payload)
    rescue
      _ -> :ok
    end
  end
end

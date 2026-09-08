defmodule Matrix.ComponentTest do
  use ExUnit.Case, async: false

  alias Matrix.{CallCtx, Component, Loopback}
  alias Matrix.Errors.{BusinessError, DepError}

  defmodule Echo do
    use Matrix.Handler

    @impl true
    def on_call(_ctx, _ticket, _cap, input, _ref), do: {:ok, %{"echo" => input}}
  end

  defmodule TicketEcho do
    use Matrix.Handler

    @impl true
    def on_call(_ctx, ticket, _cap, _input, _ref), do: {:ok, %{"ticket" => ticket}}
  end

  defmodule Gate do
    use Matrix.Handler

    @impl true
    def on_call(ctx, _ticket, _cap, _input, _ref) do
      try do
        CallCtx.invoke_dependency(ctx, "bind-x", %{}, 2_000)
        {:ok, %{"unexpected" => "wire-touched"}}
      rescue
        e in DepError -> {:ok, %{"refused" => e.code}}
      end
    end
  end

  defmodule Chain do
    use Matrix.Handler

    @impl true
    def on_call(ctx, _ticket, _cap, _input, _ref) do
      [first | _] = CallCtx.dependencies(ctx)
      out = CallCtx.invoke_dependency(ctx, first.id, %{"v" => 1}, 5_000)
      {:ok, %{"got" => out}}
    end
  end

  defmodule Slow do
    use Matrix.Handler

    @impl true
    def on_call(ctx, _ticket, _cap, input, _ref) do
      if Map.has_key?(input, "report_drops") do
        {:ok, %{"dropped" => CallCtx.event_dropped_count(ctx)}}
      else
        {:ok, %{"ok" => true}}
      end
    end

    @impl true
    def on_event(_topic, _payload) do
      # Slow observer yields; the reader keeps flowing.
      Process.sleep(30)
      :ok
    end
  end

  defmodule Bin do
    use Matrix.Handler

    @impl true
    def on_call(ctx, _ticket, _cap, _input, _ref) do
      try do
        CallCtx.send_stream(ctx, "s1", 0, <<0xFF, 0xFE>>)
        {:ok, %{}}
      rescue
        e in Matrix.Errors.SdkError -> raise %BusinessError{code: "invalid-message", message: e.message}
      end
    end
  end

  test "u64 generations keep full precision" do
    assert Matrix.Framing.gen_equal?("18446744073709551615", "18446744073709551615")
    refute Matrix.Framing.gen_equal?("18446744073709551615", "18446744073709551614")
    refute Matrix.Framing.gen_equal?("nope", "1")
  end

  test "malformed frame drops silently, session survives" do
    {path, task} =
      Loopback.serve([], [], fn conn ->
        garbage = "{oops"
        :gen_tcp.send(conn, [<<byte_size(garbage)::32-big>>, garbage])
        Loopback.send_frame(conn, Loopback.call_open("tkt-9", %{"ping" => 1}))
        ans = Loopback.read_frame(conn)
        assert ans["type"] == "call.result"
        assert ans["body"]["output"]["echo"]["ping"] == 1
        Loopback.send_frame(conn, Loopback.dispose())
        Loopback.read_frame(conn)
      end)

    {:ok, reader} = Component.connect(path, "t")
    assert Component.serve(reader, Echo) == :dispose
    assert Task.await(task, 15_000) == :ok
  end

  test "stale generation ignored, current still served" do
    {path, task} =
      Loopback.serve([], [], fn conn ->
        stale = Loopback.call_open("tkt-stale", %{}) |> Map.put("generation", "999")
        Loopback.send_frame(conn, stale)
        Loopback.send_frame(conn, Loopback.call_open("tkt-9", %{}))
        ans = Loopback.read_frame(conn)
        assert ans["body"]["ticket"] == "tkt-9"
        Loopback.send_frame(conn, Loopback.dispose())
        Loopback.read_frame(conn)
      end)

    {:ok, reader} = Component.connect(path, "t")
    assert Component.serve(reader, TicketEcho) == :dispose
    assert Task.await(task, 15_000) == :ok
  end

  test "invoke without negotiation refuses locally, wire untouched" do
    {path, task} =
      Loopback.serve([], [], fn conn ->
        Loopback.send_frame(conn, Loopback.call_open("tkt-9", %{}))
        ans = Loopback.read_frame(conn)
        assert ans["body"]["output"]["refused"] == "unsupported-feature"
        # Anything else from the SDK now would be a wire touch.
        assert {:error, :timeout} = :gen_tcp.recv(conn, 0, 1_000)
        Loopback.send_frame(conn, Loopback.dispose())
        Loopback.read_frame(conn)
      end)

    {:ok, reader} = Component.connect(path, "t")
    refute Component.has_feature?(reader, "dependency-calls/1")
    assert Component.serve(reader, Gate) == :dispose
    assert Task.await(task, 15_000) == :ok
  end

  test "dependency open/result correlate by request id" do
    bindings = [%{"binding_id" => "bind-1", "capability" => "c@1"}]

    {path, task} =
      Loopback.serve(["dependency-calls/1"], bindings, fn conn ->
        Loopback.send_frame(conn, Loopback.call_open("tkt-9", %{"chain_it" => true}))
        opened = Loopback.read_frame(conn)
        assert opened["type"] == "dependency.open"
        assert opened["body"]["binding_id"] == "bind-1"
        assert opened["body"]["parent_ticket"] == "tkt-9"
        res = Loopback.env("dependency.result", %{"status" => "ok", "output" => %{"deep" => 1}})
        res = Map.put(res, "request_id", opened["request_id"])
        Loopback.send_frame(conn, res)
        ans = Loopback.read_frame(conn)
        assert ans["body"]["output"]["got"]["deep"] == 1
        Loopback.send_frame(conn, Loopback.dispose())
        Loopback.read_frame(conn)
      end)

    {:ok, reader} = Component.connect(path, "t")
    assert Component.serve(reader, Chain) == :dispose
    assert Task.await(task, 15_000) == :ok
  end

  test "slow on_event does not stall calls; flood drops are counted" do
    {path, task} =
      Loopback.serve([], [], fn conn ->
        for i <- 0..119 do
          Loopback.send_frame(conn, Loopback.env("event.deliver", %{"topic" => "t", "payload" => %{"n" => i}}))
        end

        t0 = System.monotonic_time(:millisecond)
        Loopback.send_frame(conn, Loopback.call_open("tkt-9", %{}))
        ans = Loopback.read_frame(conn)
        assert ans["type"] == "call.result"
        assert System.monotonic_time(:millisecond) - t0 < 5_000

        Loopback.send_frame(conn, Loopback.call_open("tkt-10", %{"report_drops" => true}))
        rep = Loopback.read_frame(conn)
        assert rep["body"]["output"]["dropped"] >= 1
        Loopback.send_frame(conn, Loopback.dispose())
        Loopback.read_frame(conn)
      end)

    {:ok, reader} = Component.connect(path, "t")
    assert Component.serve(reader, Slow) == :dispose
    assert Task.await(task, 30_000) == :ok
  end

  test "non-text stream payload refused, never converted" do
    {path, task} =
      Loopback.serve([], [], fn conn ->
        Loopback.send_frame(conn, Loopback.call_open("tkt-9", %{"send_bytes" => true}))
        ans = Loopback.read_frame(conn)
        assert ans["body"]["status"] == "error"
        assert ans["body"]["error"]["code"] == "invalid-message"
        refute String.contains?(:erlang.iolist_to_binary(:json.encode(ans)), "�")
        Loopback.send_frame(conn, Loopback.dispose())
        Loopback.read_frame(conn)
      end)

    {:ok, reader} = Component.connect(path, "t")
    assert Component.serve(reader, Bin) == :dispose
    assert Task.await(task, 15_000) == :ok
  end
end

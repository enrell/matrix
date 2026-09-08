defmodule Matrix.Loopback do
  @moduledoc "In-process loopback fake host for the SDK tests (stdlib only)."

  @max_frame 1_048_576

  def send_frame(sock, map) do
    raw = :erlang.iolist_to_binary(:json.encode(map))
    :gen_tcp.send(sock, [<<byte_size(raw)::32-big>>, raw])
  end

  def read_frame(sock, timeout \\ 10_000) do
    with {:ok, <<n::32-big>>} <- recv_exact(sock, 4, timeout),
         {:ok, payload} <- recv_exact(sock, n, timeout) do
      :json.decode(payload)
    end
  end

  defp recv_exact(sock, n, timeout, acc \\ <<>>) do
    need = n - byte_size(acc)

    case :gen_tcp.recv(sock, need, timeout) do
      {:ok, data} ->
        acc = acc <> data
        if byte_size(acc) == n, do: {:ok, acc}, else: recv_exact(sock, n, timeout, acc)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def env(type, body, rid \\ "r1") do
    %{
      "protocol" => "matrix.component",
      "version" => "0.1",
      "type" => type,
      "message_id" => "m1",
      "session_id" => "s1",
      "instance_id" => "1",
      "generation" => "1",
      "request_id" => rid,
      "body" => body
    }
  end

  def call_open(ticket, input) do
    %{
      "protocol" => "matrix.component",
      "version" => "0.1",
      "type" => "call.open",
      "message_id" => "m-#{ticket}",
      "session_id" => "s1",
      "instance_id" => "1",
      "generation" => "1",
      "request_id" => "r-#{ticket}",
      "body" => %{"ticket" => ticket, "capability" => "c@1", "input" => input}
    }
  end

  def dispose do
    env("lifecycle.dispose", %{"operation_id" => "op", "deadline_ms" => 100}, "rd1")
  end

  @doc """
  Serves one connection through the handshake, then runs `script`. Returns
  {sock_path, task} where task completes with the script.
  """
  def serve(features, bindings, script) do
    dir = Path.join(System.tmp_dir!(), "exunits-#{random()}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "t.sock")
    {:ok, srv} =
      :gen_tcp.listen(0, [:binary, {:packet, :raw}, {:active, false}, {:ifaddr, {:local, String.to_charlist(path)}}])

    task =
      Task.async(fn ->
        {:ok, conn} = :gen_tcp.accept(srv)
        hello = read_frame(conn)
        true = hello["type"] == "hello"

        send_frame(conn, %{
          "protocol" => "matrix.component",
          "version" => "0.1",
          "type" => "welcome",
          "message_id" => "h1",
          "session_id" => "s1",
          "body" => %{"version" => "0.1", "max_frame" => @max_frame, "limits" => %{}, "features" => features}
        })

        reg = read_frame(conn)
        true = reg["type"] == "component.register"

        send_frame(conn, %{
          "protocol" => "matrix.component",
          "version" => "0.1",
          "type" => "registered",
          "message_id" => "r",
          "session_id" => "s1",
          "instance_id" => "1",
          "generation" => "1",
          "body" => %{"logical" => "t"}
        })

        act =
          env("lifecycle.activate", %{
            "operation_id" => "op",
            "manifest" => %{},
            "bindings" => [],
            "dependency_bindings" => bindings
          }, "q")

        send_frame(conn, act)
        lc = read_frame(conn)
        true = lc["type"] == "lifecycle.result"
        script.(conn)
        :gen_tcp.close(conn)
        :gen_tcp.close(srv)
        File.rm_rf(dir)
        :ok
      end)

    {path, task}
  end

  defp random, do: :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
end

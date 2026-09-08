defmodule Matrix.OperatorTest do
  use ExUnit.Case, async: false

  alias Matrix.Operator
  alias Matrix.Operator.{Client, OwnedKernel}
  alias Matrix.Errors.{BootstrapError, OperatorError}

  defp write_fake_binary do
    dir = Path.join(System.tmp_dir!(), "opfake-#{random()}")
    File.mkdir_p!(dir)
    fake = Path.join(dir, "matrix-managed")

    File.write!(fake, """
    #!/bin/sh
    if [ "$1" = "request" ]; then
      case "$7" in
        *sleep*) exec sleep 30;;
        *badjson*) echo 'not json';;
        *denied*) echo 'permission-denied: nope' >&2; exit 1;;
        *) echo '{"ok":true}';;
      esac
    else echo 'usage: matrix-managed serve <config>' >&2; exit 1
    fi
    """)

    File.chmod!(fake, 0o755)
    for n <- ["ca", "cert", "key"], do: File.write!(Path.join(dir, n), "")
    dir
  end

  defp pki(dir),
    do: %{ca: Path.join(dir, "ca"), cert: Path.join(dir, "cert"), key: Path.join(dir, "key")}

  defp random, do: :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)

  test "maps CLI outcomes to typed errors" do
    dir = write_fake_binary()

    try do
      fake = Path.join(dir, "matrix-managed")
      client = Operator.connect(fake, "127.0.0.1:9", pki(dir))
      assert Operator.request(client, %{"action" => "ping"}) == %{"ok" => true}

      assert_raise OperatorError, ~r/permission-denied/, fn ->
        Operator.request(client, %{"action" => "denied-op"})
      end

      try do
        Operator.request(client, %{"action" => "badjson"})
        flunk("badjson succeeded")
      rescue
        e in OperatorError -> assert e.code == "internal"
      end

      try do
        Operator.request(client, %{"action" => "sleep"}, 1_000)
        flunk("sleep succeeded")
      rescue
        e in OperatorError -> assert e.code == "outcome-unknown"
      end

      assert_raise OperatorError, fn ->
        Operator.request(client, %{"no-action" => true})
      end

      closed = Operator.close_client(client)

      assert_raise Matrix.Errors.SdkError, fn ->
        Operator.request(closed, %{"action" => "ping"})
      end
    after
      File.rm_rf(dir)
    end
  end

  test "bootstrap phases are explicit" do
    assert_raise BootstrapError, fn ->
      Operator.start("/nonexistent/matrix-managed", %{"home" => "/tmp/x"}, %{ca: "", cert: "", key: ""})
    end

    try do
      Operator.start("/bin/true", %{"components" => []}, %{ca: "", cert: "", key: ""})
      flunk("homeless config started")
    rescue
      e in BootstrapError -> assert e.phase == "config"
    end

    assert_raise BootstrapError, fn ->
      Operator.connect("/nonexistent/x", "127.0.0.1:1", %{ca: "a", cert: "b", key: "c"})
    end
  end

  test "doctor shape carries no secrets" do
    rep = Operator.doctor("/nonexistent/binary")

    for k <- ["elixir", "otp", "binary", "binary_found", "cli_shape_ok", "openssl", "bwrap",
              "socket_dir_writable", "errors"] do
      assert Map.has_key?(rep, k), k
    end

    refute String.contains?(jason_encode(rep) |> String.replace("socket_dir_writable", ""), "lease")

    if bin = System.get_env("MX_MATRIX_MANAGED") do
      rep2 = Operator.doctor(bin)
      assert rep2["binary_found"] and rep2["cli_shape_ok"]
    end
  end

  defp jason_encode(term), do: :erlang.iolist_to_binary(:json.encode(term))

  @tag :live
  test "start/attach lifecycle with stable denial codes" do
    bin = System.get_env("MX_MATRIX_MANAGED") || flunk("needs MX_MATRIX_MANAGED")
    pki_script = System.get_env("MX_DEV_PKI") || flunk("needs MX_DEV_PKI")
    tmp = Path.join(System.tmp_dir!(), "oplive-#{random()}")
    File.mkdir_p!(tmp)

    try do
      pki = Path.join(tmp, "pki")
      {_out, 0} = System.cmd("python3", [pki_script, pki, "--server-name", "localhost"])

      fp =
        :crypto.hash(:sha256, File.read!(Path.join(pki, "client.der")))
        |> Base.encode16(case: :lower)

      cfg = %{
        "home" => Path.join(tmp, "home"),
        "components" => [
          %{
            "manifest" => %{"id" => "echo", "capabilities" => ["echo.msg@1"], "reducer" => "echo"},
            "trusted" => true
          }
        ],
        "grants" => %{fp => %{"components" => ["echo"], "capabilities" => ["echo.msg@1"]}},
        "tls" => %{
          "listen" => "127.0.0.1:0",
          "ca" => Path.join(pki, "ca.der"),
          "cert" => Path.join(pki, "server.der"),
          "key" => Path.join(pki, "server-key.der")
        }
      }

      opki = %{ca: Path.join(pki, "ca.der"), cert: Path.join(pki, "client.der"), key: Path.join(pki, "client-key.der")}
      kernel = Operator.start(bin, cfg, opki)
      assert String.starts_with?(kernel.api, "0.1.")

      act = Operator.activate(kernel.client, "echo", 20_000)
      %{"lease" => lease, "fence" => fence} = act
      v = Operator.invoke(kernel.client, lease, fence, "op-live-1", "echo.msg@1", %{"ping" => 1})
      assert v["ok"] == true

      attached = Operator.connect(bin, kernel.client.listen, opki)
      v2 = Operator.invoke(attached, lease, fence, "op-live-2", "echo.msg@1", %{})
      assert v2["ok"] == true
      # Attachment owns nothing: daemon keeps serving.
      _ = Operator.close_client(attached)
      v3 = Operator.invoke(kernel.client, lease, fence, "op-live-3", "echo.msg@1", %{})
      assert v3["ok"] == true

      assert_raise OperatorError, fn ->
        Operator.invoke(kernel.client, "dead", "1", "op-x", "echo.msg@1", %{})
      end

      Operator.release(kernel.client, lease, fence)
      closed = Operator.close(kernel)
      assert closed.closed

      assert_raise Matrix.Errors.SdkError, fn ->
        Operator.invoke(closed.client, lease, fence, "op-x", "echo.msg@1", %{})
      end
    after
      File.rm_rf(tmp)
    end
  end
end

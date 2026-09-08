defmodule Matrix.Operator do
  @moduledoc """
  Operator/application surface for Elixir (ML1, stdlib only).

  Same contract as the other SDKs: calls travel through the staged
  `matrix-managed` binary (`serve` to own a kernel, `request` for
  authenticated admin actions over mutual TLS). `start/3` owns its
  process; `connect/4` only attaches. Server identity never implies
  caller authority: the operator PKI map is always explicit.

  Supervision note (epic row): a `Matrix.Component` reader is linked to
  the process that connected it — if that process dies, the session
  goes down with it. The SDK never resurrects an old generation: a
  restarted owner connects again and gets fresh leases.

  Process lifecycle uses OS signals via the `kill` binary (Linux, like
  the rest of the managed profile): SIGTERM for graceful shutdown,
  SIGKILL past the deadline. `Port.close/1` alone never signals.
  """

  alias Matrix.Errors.{BootstrapError, OperatorError}

  @expected_api_prefix "0.1."
  @ready_timeout_ms 30_000
  @stop_timeout_ms 5_000
  @request_slack_ms 10_000

  @known_codes ~w[
    permission-denied stale-generation outcome-unknown
    unauthenticated invalid-message unsupported-version
    dependency-unavailable ambiguous-provider
    context-not-active resource-exhausted deadline-exceeded
    cancelled cleanup-pending internal
  ]

  defmodule Client do
    @moduledoc """
    Attached operator client. `close_client/1` marks the handle closed
    and never stops any daemon.
    """
    defstruct [:binary, :listen, :pki, :server_name, :owned, closed: false]

    @type t :: %__MODULE__{}
  end

  defmodule OwnedKernel do
    @moduledoc """
    A kernel this application started and owns. `close/1` is idempotent
    and reaps exactly the spawned daemon plus the private directory.
    """
    defstruct [:port, :workdir, :client, :epoch, :api, :profile, closed: false]

    @type t :: %__MODULE__{}
  end

  # -- requests ------------------------------------------------------------

  @doc "One authenticated admin action. Refusals keep wire codes."
  def request(client, action, timeout_ms \\ 30_000)

  def request(%Client{closed: true}, _action, _timeout) do
    raise %Matrix.Errors.SdkError{code: "internal", phase: "client", message: "client is closed"}
  end

  def request(%Client{} = client, action, timeout_ms) when is_map(action) do
    unless is_binary(action["action"]) do
      raise %OperatorError{code: "invalid-message", message: "action map with 'action' required"}
    end

    args = [
      "request",
      client.pki.ca,
      client.pki.cert,
      client.pki.key,
      client.listen,
      client.server_name,
      # :json.encode returns iodata: flatten for argv.
      :erlang.iolist_to_binary(:json.encode(action))
    ]

    # `request` prints pure JSON on stdout on success; denials go to
    # stderr with nonzero exit — merged capture keeps both readable.
    case run_capture(client.binary, args, timeout_ms + @request_slack_ms) do
      {:ok, 0, text} ->
        case decode(text) do
          {:ok, decoded} -> decoded
          :error -> raise %OperatorError{code: "internal", message: "undecodable response"}
        end

      {:ok, _code, text} ->
        text = String.trim(text)
        first = text |> String.split("\n") |> List.first("") |> String.trim()

        raise %OperatorError{
          code: guess_code(text),
          message: if(first == "", do: "request refused", else: first)
        }

      {:timeout} ->
        raise %OperatorError{
          code: "outcome-unknown",
          message: "admin action timed out after #{timeout_ms}ms (not retried)"
        }

      {:spawn, reason} ->
        raise %OperatorError{code: "transport", message: "spawn: #{reason}"}
    end
  end

  @doc "Provisions a lease for a component."
  def activate(client, component, ttl_ms \\ 20_000, timeout_ms \\ 30_000) do
    request(client, %{"action" => "activate", "component" => component, "ttl_ms" => ttl_ms}, timeout_ms)
  end

  @doc "Queries a lease."
  def status(client, lease, fence, timeout_ms \\ 30_000) do
    request(client, %{"action" => "status", "lease" => lease, "fence" => to_string(fence)}, timeout_ms)
  end

  @doc "Calls a capability under a lease."
  def invoke(client, lease, fence, operation, cap, input, timeout_ms \\ 30_000) do
    request(
      client,
      %{
        "action" => "invoke",
        "lease" => lease,
        "fence" => to_string(fence),
        "operation" => operation,
        "cap" => cap,
        "input" => input || %{}
      },
      timeout_ms
    )
  end

  @doc "Retires a lease."
  def release(client, lease, fence, timeout_ms \\ 30_000) do
    request(client, %{"action" => "release", "lease" => lease, "fence" => to_string(fence)}, timeout_ms)
  end

  @doc "Rotates a lease."
  def renew(client, lease, fence, ttl_ms \\ 20_000, timeout_ms \\ 30_000) do
    request(
      client,
      %{"action" => "renew", "lease" => lease, "fence" => to_string(fence), "ttl_ms" => ttl_ms},
      timeout_ms
    )
  end

  @doc "Polls status until the session reports ready."
  def wait_ready(client, lease, fence, timeout_ms \\ 20_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_ready_loop(client, lease, fence, deadline, nil)
  end

  defp wait_ready_loop(client, lease, fence, deadline, last) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise %OperatorError{
        code: "outcome-unknown",
        message: "session not ready in budget (last=#{inspect(redact(last))})"
      }
    end

    st = status(client, lease, fence, 5_000)

    if st["ready"] == true do
      st
    else
      Process.sleep(100)
      wait_ready_loop(client, lease, fence, deadline, st)
    end
  end

  @doc "Marks a client closed. Never stops any daemon."
  def close_client(%Client{} = client), do: %{client | closed: true}

  # -- bootstrap -----------------------------------------------------------

  @doc """
  Attaches to an existing kernel. The client owns no process: closing
  never shuts the daemon down.
  """
  def connect(binary, listen, pki, server_name \\ "localhost") do
    for {label, path} <- [{"binary", binary}, {"ca", pki.ca}, {"cert", pki.cert}, {"key", pki.key}] do
      unless is_binary(path) and File.exists?(path) do
        raise %BootstrapError{code: "transport", phase: "connect", message: "#{label} not found: #{path}"}
      end
    end

    unless is_binary(listen) and listen != "" do
      raise %BootstrapError{code: "invalid-message", phase: "connect", message: "listen address required"}
    end

    %Client{binary: binary, listen: listen, pki: pki, server_name: server_name || "localhost", owned: false}
  end

  @doc """
  Starts an owned kernel from a config map. The operator PKI is the
  caller's identity and is required; failures reap everything created.
  """
  def start(binary, config, pki, server_name \\ "localhost") do
    unless is_binary(binary) and File.regular?(binary) and executable?(binary) do
      raise %BootstrapError{code: "transport", phase: "spawn", message: "binary not executable: #{binary}"}
    end

    unless is_map(config) and is_binary(config["home"]) and config["home"] != "" do
      raise %BootstrapError{code: "invalid-message", phase: "config", message: "config map with 'home' required"}
    end

    unless is_binary(pki.ca) and pki.ca != "" and is_binary(pki.cert) and pki.cert != "" and
             is_binary(pki.key) and pki.key != "" do
      raise %BootstrapError{
        code: "invalid-message",
        phase: "config",
        message: "operator PKI (ca/cert/key) is required: server identity never implies caller authority"
      }
    end

    for {label, path} <- [{"ca", pki.ca}, {"cert", pki.cert}, {"key", pki.key}] do
      unless File.exists?(path) do
        raise %BootstrapError{
          code: "invalid-message",
          phase: "config",
          message: "operator #{label} not found: #{path}"
        }
      end
    end

    workdir = Path.join(System.tmp_dir!(), "mx-ex-#{random_hex()}")
    File.mkdir_p!(workdir)
    cfg_path = Path.join(workdir, "config.json")
    File.write!(cfg_path, :erlang.iolist_to_binary(:json.encode(config)))

    port =
      try do
        Port.open({:spawn_executable, binary}, [:binary, :exit_status, {:args, ["serve", cfg_path]}])
      rescue
        e ->
          File.rm_rf(workdir)
          reraise %BootstrapError{code: "transport", phase: "spawn", message: "spawn: #{Exception.message(e)}"},
                  __STACKTRACE__
      end

    try do
      ready = read_ready(port)

      listen =
        case ready["listen"] do
          l when is_binary(l) and l != "" -> l
          _ -> get_in(config, ["tls", "listen"]) || ""
        end

      api = ready["api"] || ""

      unless api == "" or String.starts_with?(api, @expected_api_prefix) do
        raise %BootstrapError{
          code: "unsupported-version",
          phase: "version",
          message: "binary api outside #{@expected_api_prefix}x: #{api}"
        }
      end

      client = %Client{
        binary: binary,
        listen: listen,
        pki: pki,
        server_name: server_name || "localhost",
        owned: true
      }

      %OwnedKernel{
        port: port,
        workdir: workdir,
        client: client,
        epoch: ready["epoch"],
        api: api,
        profile: ready["profile"] || ""
      }
    rescue
      e in BootstrapError ->
        kill_port(port)
        File.rm_rf(workdir)
        reraise e, __STACKTRACE__
    catch
      _, _ ->
        kill_port(port)
        File.rm_rf(workdir)
        raise %BootstrapError{code: "transport", phase: "spawn", message: "start failed"}
    end
  end

  @doc """
  Idempotent close: SIGTERM, bounded wait, SIGKILL, remove the private
  directory. Reaps exactly the spawned daemon.
  """
  def close(%OwnedKernel{closed: true} = kernel), do: kernel

  def close(%OwnedKernel{} = kernel) do
    client = close_client(kernel.client)

    if kernel.port && Port.info(kernel.port) do
      signal_port(kernel.port, "-TERM")

      unless await_exit(kernel.port, @stop_timeout_ms) do
        signal_port(kernel.port, "-KILL")
        await_exit(kernel.port, @stop_timeout_ms)
      end

      if Port.info(kernel.port), do: Port.close(kernel.port)
    end

    File.rm_rf(kernel.workdir)
    %{kernel | client: client, closed: true}
  end

  # -- doctor --------------------------------------------------------------

  @doc "Environment diagnosis (no secrets)."
  def doctor(binary) do
    {found, executable, shape_ok, errors} =
      if is_binary(binary) and File.regular?(binary) do
        executable = executable?(binary)

        {shape_ok, errors} =
          if executable do
            case run_capture(binary, [], 10_000) do
              {:ok, _code, text} ->
                if String.contains?(text, "matrix-managed serve") do
                  {true, []}
                else
                  {false, ["binary does not speak the managed CLI shape"]}
                end

              _ ->
                {false, ["binary probe failed"]}
            end
          else
            {false, ["binary not executable"]}
          end

        {true, executable, shape_ok, errors}
      else
        {false, false, false,
         ["binary not found: set it explicitly or via PATH (no silent download)"]}
      end

    {sock_ok, errors} =
      case socket_probe() do
        :ok -> {true, errors}
        {:error, reason} -> {false, ["unix socket probe failed: #{reason}" | errors]}
      end

    %{
      "elixir" => System.version(),
      "otp" => to_string(:erlang.system_info(:otp_release)),
      "binary" => binary || "",
      "binary_found" => found,
      "binary_executable" => executable,
      "cli_shape_ok" => shape_ok,
      "openssl" => !is_nil(System.find_executable("openssl")),
      "bwrap" => !is_nil(System.find_executable("bwrap")),
      "socket_dir_writable" => sock_ok,
      "errors" => Enum.reverse(errors)
    }
  end

  # -- private -------------------------------------------------------------

  defp executable?(path) do
    case File.stat(path) do
      {:ok, %File.Stat{mode: mode}} -> Bitwise.band(mode, 0o111) != 0
      _ -> false
    end
  end

  defp random_hex, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)

  defp guess_code(text) do
    low = String.downcase(text || "")

    Enum.find(@known_codes, fn c -> String.contains?(low, c) end) ||
      if String.trim(text || "") == "", do: "internal", else: "transport"
  end

  defp decode(binary) do
    {:ok, :json.decode(binary)}
  rescue
    _ -> :error
  end

  defp redact(nil), do: nil

  defp redact(map) when is_map(map) do
    Map.new(map, fn
      {k, _} when k in ["lease", "launch_token", "token"] -> {k, "<redacted>"}
      {k, v} -> {k, redact(v)}
    end)
  end

  defp redact(list) when is_list(list), do: Enum.map(list, &redact/1)
  defp redact(other), do: other

  # Runs a binary with an absolute deadline (merged streams: `request`
  # prints pure JSON on success). Never blocks past the deadline.
  defp run_capture(binary, args, timeout_ms) do
    task = Task.async(fn -> System.cmd(binary, args, stderr_to_stdout: true) end)

    case Task.yield(task, timeout_ms) do
      {:ok, {text, code}} -> {:ok, code, text}
      {:exit, reason} -> {:spawn, inspect(reason)}
      nil -> Task.shutdown(task, :brutal_kill); {:timeout}
    end
  rescue
    e -> {:spawn, Exception.message(e)}
  end

  # -- port lifecycle (signals; Port.close alone never signals) ------------

  defp os_pid(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} -> pid
      _ -> nil
    end
  end

  defp signal_port(port, sig) do
    case os_pid(port) do
      nil -> :ok
      pid -> System.cmd("kill", [sig, to_string(pid)], stderr_to_stdout: true); :ok
    end
  rescue
    _ -> :ok
  end

  defp kill_port(port) do
    signal_port(port, "-KILL")
    await_exit(port, @stop_timeout_ms)
    if Port.info(port), do: Port.close(port)
    :ok
  end

  defp await_exit(port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    await_loop(port, deadline)
  end

  defp await_loop(port, deadline) do
    receive do
      {^port, {:exit_status, _}} -> true
    after
      100 ->
        cond do
          is_nil(Port.info(port)) -> true
          System.monotonic_time(:millisecond) >= deadline -> false
          true -> await_loop(port, deadline)
        end
    end
  end

  # -- ready line ----------------------------------------------------------

  defp read_ready(port) do
    deadline = System.monotonic_time(:millisecond) + @ready_timeout_ms
    read_ready_loop(port, deadline, "")
  end

  defp read_ready_loop(port, deadline, acc) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, data}} ->
        acc = acc <> data

        case String.split(acc, "\n", parts: 2) do
          [line, _rest] -> parse_ready(port, line)
          [_partial] -> read_ready_loop(port, deadline, acc)
        end

      {^port, {:exit_status, code}} ->
        raise %BootstrapError{
          code: "internal",
          phase: "config",
          message: "daemon refused config: exit #{code} #{String.slice(acc, 0, 160)}"
        }
    after
      remaining ->
        kill_port(port)

        raise %BootstrapError{
          code: "transport",
          phase: "ready",
          message: "no ready line in budget"
        }
    end
  end

  defp parse_ready(port, line) do
    case decode(line) do
      {:ok, ready} when is_map(ready) ->
        if ready["ready"] == true do
          ready
        else
          kill_port(port)

          raise %BootstrapError{
            code: "transport",
            phase: "ready",
            message: "daemon not ready: #{String.slice(line, 0, 160)}"
          }
        end

      :error ->
        kill_port(port)

        raise %BootstrapError{
          code: "transport",
          phase: "ready",
          message: "undecodable ready line: #{String.slice(line, 0, 120)}"
        }
    end
  end

  defp socket_probe do
    dir = Path.join(System.tmp_dir!(), "mx-doc-#{random_hex()}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "t.sock")

    try do
      case :gen_tcp.listen(0, [:binary, {:packet, :raw}, {:ifaddr, {:local, String.to_charlist(path)}}]) do
        {:ok, sock} ->
          :gen_tcp.close(sock)
          :ok

        {:error, reason} ->
          {:error, inspect(reason)}
      end
    after
      File.rm_rf(dir)
    end
  end
end

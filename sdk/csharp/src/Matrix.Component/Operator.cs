// Operator/application surface for .NET (ML1, no packages).
//
// Same contract as the other SDKs: calls travel through the staged
// matrix-managed binary (serve to own a kernel, request for
// authenticated admin actions over mutual TLS). Start owns its
// process; Attach only connects. Server identity never implies caller
// authority: OperatorPki is always explicit.
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Matrix.Component;

/// <summary>Caller's mTLS identity (never the server's).</summary>
public sealed class OperatorPki
{
    public string Ca { get; init; } = "";
    public string Cert { get; init; } = "";
    public string Key { get; init; } = "";
}

/// <summary>Attached operator client. Close never stops any daemon.</summary>
public sealed class Client
{
    private static readonly string[] KnownCodes =
    {
        "permission-denied", "stale-generation", "outcome-unknown",
        "unauthenticated", "invalid-message", "unsupported-version",
        "dependency-unavailable", "ambiguous-provider",
        "context-not-active", "resource-exhausted", "deadline-exceeded",
        "cancelled", "cleanup-pending", "internal",
    };

    private readonly string _binary;
    private readonly string _listen;
    private readonly OperatorPki _pki;
    private readonly string _serverName;
    private bool _closed;

    internal Client(string binary, string listen, OperatorPki pki, string serverName, bool owned)
    {
        _binary = binary;
        _listen = listen;
        _pki = pki;
        _serverName = serverName;
        Owned = owned;
    }

    public bool Owned { get; }

    private void CheckOpen()
    {
        if (_closed)
            throw new SdkError("internal", "client", "client is closed");
    }

    private static string GuessCode(string stderr)
    {
        var low = (stderr ?? "").ToLowerInvariant();
        foreach (var c in KnownCodes)
            if (low.Contains(c))
                return c;
        return (stderr ?? "").Trim() != "" ? "transport" : "internal";
    }

    /// <summary>One authenticated admin action; refusals keep wire codes.</summary>
    public Dictionary<string, JsonElement> Request(Dictionary<string, object?> action,
        TimeSpan timeout = default)
    {
        CheckOpen();
        if (action == null || !action.ContainsKey("action"))
            throw new OperatorError("invalid-message", "action map with 'action' required");
        if (timeout <= TimeSpan.Zero)
            timeout = TimeSpan.FromSeconds(30);
        var psi = new ProcessStartInfo(_binary)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        // No shell: the JSON travels as one argv element.
        psi.ArgumentList.Add("request");
        psi.ArgumentList.Add(_pki.Ca);
        psi.ArgumentList.Add(_pki.Cert);
        psi.ArgumentList.Add(_pki.Key);
        psi.ArgumentList.Add(_listen);
        psi.ArgumentList.Add(_serverName);
        psi.ArgumentList.Add(JsonSerializer.Serialize(action));
        using var proc = Process.Start(psi) ??
            throw new OperatorError("transport", "spawn: " + _binary);
        var stdout = new StringBuilder();
        var stderr = new StringBuilder();
        proc.OutputDataReceived += (_, e) => { if (e.Data != null) stdout.AppendLine(e.Data); };
        proc.ErrorDataReceived += (_, e) => { if (e.Data != null) stderr.AppendLine(e.Data); };
        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();
        if (!proc.WaitForExit((int)(timeout + TimeSpan.FromSeconds(10)).TotalMilliseconds))
        {
            try { proc.Kill(entireProcessTree: true); } catch { /* gone */ }
            proc.WaitForExit(5000);
            throw new OperatorError("outcome-unknown",
                $"admin action timed out after {timeout.TotalSeconds}s (not retried)");
        }
        proc.WaitForExit(); // drain async readers after exit (no deadlock: process is gone)
        var err = (stderr.ToString() + stdout.ToString()).Trim();
        if (proc.ExitCode != 0)
        {
            var first = err.Split('\n')[0].Trim();
            throw new OperatorError(GuessCode(err),
                first != "" ? first : "request refused");
        }
        try
        {
            using var doc = JsonDocument.Parse(stdout.ToString());
            var decoded = new Dictionary<string, JsonElement>();
            foreach (var p in doc.RootElement.EnumerateObject())
                decoded[p.Name] = p.Value.Clone();
            return decoded;
        }
        catch (JsonException e)
        {
            throw new OperatorError("internal", "undecodable response: " + e.Message);
        }
    }

    public Dictionary<string, JsonElement> Activate(string component, long ttlMs = 20000,
        TimeSpan timeout = default) =>
        Request(new Dictionary<string, object?>
            { ["action"] = "activate", ["component"] = component, ["ttl_ms"] = ttlMs }, timeout);

    public Dictionary<string, JsonElement> Status(string lease, string fence,
        TimeSpan timeout = default) =>
        Request(new Dictionary<string, object?>
            { ["action"] = "status", ["lease"] = lease, ["fence"] = fence }, timeout);

    public Dictionary<string, JsonElement> Invoke(string lease, string fence, string operation,
        string cap, object? input, TimeSpan timeout = default) =>
        Request(new Dictionary<string, object?>
        {
            ["action"] = "invoke", ["lease"] = lease, ["fence"] = fence,
            ["operation"] = operation, ["cap"] = cap,
            ["input"] = input ?? new Dictionary<string, object?>(),
        }, timeout);

    public Dictionary<string, JsonElement> Release(string lease, string fence,
        TimeSpan timeout = default) =>
        Request(new Dictionary<string, object?>
            { ["action"] = "release", ["lease"] = lease, ["fence"] = fence }, timeout);

    public Dictionary<string, JsonElement> Renew(string lease, string fence, long ttlMs = 20000,
        TimeSpan timeout = default) =>
        Request(new Dictionary<string, object?>
            { ["action"] = "renew", ["lease"] = lease, ["fence"] = fence, ["ttl_ms"] = ttlMs },
            timeout);

    /// <summary>Polls status until the session reports ready.</summary>
    public async Task<Dictionary<string, JsonElement>> WaitReadyAsync(string lease, string fence,
        TimeSpan timeout = default, CancellationToken ct = default)
    {
        if (timeout <= TimeSpan.Zero)
            timeout = TimeSpan.FromSeconds(20);
        var end = DateTime.UtcNow + timeout;
        Dictionary<string, JsonElement>? last = null;
        while (DateTime.UtcNow < end)
        {
            last = await Task.Run(() => Status(lease, fence, TimeSpan.FromSeconds(5)), ct)
                .ConfigureAwait(false);
            if (last.TryGetValue("ready", out var r) && r.ValueKind == JsonValueKind.True)
                return last;
            await Task.Delay(100, ct).ConfigureAwait(false);
        }
        throw new OperatorError("outcome-unknown", "session not ready in budget");
    }

    /// <summary>Marks this handle closed. Never stops any daemon.</summary>
    public void Close() => _closed = true;
}

/// <summary>A kernel this application started and owns.</summary>
public sealed class OwnedKernel : IAsyncDisposable
{
    private readonly Process _proc;
    private readonly string _workdir;
    private bool _closed;

    public Client Client { get; }
    public string Listen { get; }
    public string Epoch { get; }
    public string Api { get; }
    public string Profile { get; }

    internal OwnedKernel(Process proc, string workdir, Client client, string listen,
        string epoch, string api, string profile)
    {
        _proc = proc;
        _workdir = workdir;
        Client = client;
        Listen = listen;
        Epoch = epoch;
        Api = api;
        Profile = profile;
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int sig);

    /// <summary>
    /// Idempotent: SIGTERM, bounded wait, SIGKILL, remove the private
    /// directory. Reaps exactly the spawned daemon.
    /// </summary>
    public async Task CloseAsync()
    {
        if (_closed)
            return;
        _closed = true;
        Client.Close();
        try
        {
            if (!_proc.HasExited)
            {
                try
                {
                    // Graceful first (the daemon reports shutdown); the
                    // P/Invoke is libc-only, no package.
                    if (kill(_proc.Id, 15) != 0)
                        _proc.Kill();
                }
                catch { try { _proc.Kill(); } catch { /* gone */ } }
                var exited = await Task.Run(() =>
                {
                    try { return _proc.WaitForExit(5000); }
                    catch { return true; }
                }).ConfigureAwait(false);
                if (!exited)
                {
                    try { _proc.Kill(entireProcessTree: true); } catch { /* gone */ }
                    await Task.Run(() =>
                    {
                        try { _proc.WaitForExit(5000); } catch { /* give up; OS reaps */ }
                    }).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            try { Directory.Delete(_workdir, recursive: true); } catch { /* best effort */ }
            _proc.Dispose();
        }
    }

    public ValueTask DisposeAsync() => new(CloseAsync());
}

/// <summary>Kernel bootstrap: owned start vs shared attach.</summary>
public static class Kernel
{
    public const string ExpectedApiPrefix = "0.1.";
    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Attaches to an existing kernel. The client owns no process:
    /// Close never shuts the daemon down.
    /// </summary>
    public static Client Attach(string binary, string listen, OperatorPki pki,
        string serverName = "localhost")
    {
        foreach (var (label, path) in new[]
            { ("binary", binary), ("ca", pki.Ca), ("cert", pki.Cert), ("key", pki.Key) })
            if (string.IsNullOrEmpty(path) || !File.Exists(path))
                throw new BootstrapError("transport", "connect", $"{label} not found: {path}");
        if (string.IsNullOrEmpty(listen))
            throw new BootstrapError("invalid-message", "connect", "listen address required");
        return new Client(binary, listen, pki, serverName == "" ? "localhost" : serverName, false);
    }

    /// <summary>
    /// Starts an owned kernel from a config map. operatorPki is the
    /// caller's identity and is required; failures reap everything created.
    /// </summary>
    public static async Task<OwnedKernel> StartAsync(string binary,
        Dictionary<string, object?> config, OperatorPki operatorPki,
        string serverName = "localhost", CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(binary) || !File.Exists(binary))
            throw new BootstrapError("transport", "spawn", $"binary not executable: {binary}");
        if (config == null || !config.TryGetValue("home", out var home) || home is not string ||
            string.IsNullOrEmpty((string)home))
            throw new BootstrapError("invalid-message", "config", "config map with 'home' required");
        if (string.IsNullOrEmpty(operatorPki.Ca) || string.IsNullOrEmpty(operatorPki.Cert) ||
            string.IsNullOrEmpty(operatorPki.Key))
            throw new BootstrapError("invalid-message", "config",
                "operator PKI (ca/cert/key) is required: server identity never implies caller authority");
        foreach (var (label, path) in new[]
            { ("ca", operatorPki.Ca), ("cert", operatorPki.Cert), ("key", operatorPki.Key) })
            if (!File.Exists(path))
                throw new BootstrapError("invalid-message", "config",
                    $"operator {label} not found: {path}");
        var workdir = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "mx-cs-" + Path.GetRandomFileName())).FullName;
        Process? proc = null;
        try
        {
            var cfgPath = Path.Combine(workdir, "config.json");
            await File.WriteAllTextAsync(cfgPath, JsonSerializer.Serialize(config), ct)
                .ConfigureAwait(false);
            var psi = new ProcessStartInfo(binary)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            psi.ArgumentList.Add("serve");
            psi.ArgumentList.Add(cfgPath);
            proc = Process.Start(psi) ??
                throw new BootstrapError("transport", "spawn", "spawn: " + binary);
            var ready = await ReadReadyAsync(proc, ReadyTimeout, ct).ConfigureAwait(false);
            var listen = ready.TryGetValue("listen", out var l) && l.ValueKind == JsonValueKind.String
                ? l.GetString() ?? "" : "";
            if (listen == "" && config.TryGetValue("tls", out var tls) && tls is JsonElement)
            { /* server-side only; operator address came from the ready line */ }
            var api = ready.TryGetValue("api", out var a) && a.ValueKind == JsonValueKind.String
                ? a.GetString() ?? "" : "";
            if (api != "" && !api.StartsWith(ExpectedApiPrefix, StringComparison.Ordinal))
                throw new BootstrapError("unsupported-version", "version",
                    $"binary api outside {ExpectedApiPrefix}x: {api}");
            var epoch = ready.TryGetValue("epoch", out var e) ? e.ToString() : "";
            var profile = ready.TryGetValue("profile", out var p) && p.ValueKind == JsonValueKind.String
                ? p.GetString() ?? "" : "";
            var client = new Client(binary, listen, operatorPki,
                serverName == "" ? "localhost" : serverName, true);
            return new OwnedKernel(proc, workdir, client, listen, epoch, api, profile);
        }
        catch
        {
            if (proc != null && !proc.HasExited)
            {
                try { proc.Kill(entireProcessTree: true); } catch { /* gone */ }
            }
            try { Directory.Delete(workdir, recursive: true); } catch { /* best effort */ }
            proc?.Dispose();
            throw;
        }
    }

    private static async Task<Dictionary<string, JsonElement>> ReadReadyAsync(
        Process proc, TimeSpan timeout, CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linked.CancelAfter(timeout);
        string? line = null;
        try
        {
            line = await proc.StandardOutput.ReadLineAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new BootstrapError("transport", "ready", "no ready line in budget");
        }
        if (line == null)
        {
            string err = "";
            try { err = await proc.StandardError.ReadToEndAsync().ConfigureAwait(false); }
            catch { /* best effort */ }
            var first = err.Trim().Split('\n')[0].Trim();
            throw new BootstrapError("internal", "config",
                "daemon refused config: " + (first != "" ? first : $"exit {proc.ExitCode}"));
        }
        Dictionary<string, JsonElement> ready;
        try
        {
            using var doc = JsonDocument.Parse(line);
            ready = new Dictionary<string, JsonElement>();
            foreach (var p in doc.RootElement.EnumerateObject())
                ready[p.Name] = p.Value.Clone();
        }
        catch (JsonException)
        {
            throw new BootstrapError("transport", "ready", "undecodable ready line");
        }
        if (!ready.TryGetValue("ready", out var r) || r.ValueKind != JsonValueKind.True)
            throw new BootstrapError("transport", "ready", "daemon not ready");
        return ready;
    }

    private static bool HasCliShape(string binary)
    {
        try
        {
            var psi = new ProcessStartInfo(binary)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi);
            if (proc == null)
                return false;
            proc.WaitForExit(10000);
            var text = proc.StandardError.ReadToEnd() + proc.StandardOutput.ReadToEnd();
            return text.Contains("matrix-managed serve");
        }
        catch
        {
            return false;
        }
    }

    /// <summary>Environment diagnosis (no secrets).</summary>
    public static Dictionary<string, object?> Doctor(string? binary)
    {
        var errors = new List<string>();
        var rep = new Dictionary<string, object?>
        {
            ["dotnet"] = Environment.Version.ToString(),
            ["binary"] = binary ?? "",
            ["binary_found"] = false,
            ["binary_executable"] = false,
            ["cli_shape_ok"] = false,
            ["openssl"] = OnPath("openssl"),
            ["bwrap"] = OnPath("bwrap"),
            ["socket_dir_writable"] = false,
            ["errors"] = errors,
        };
        if (!string.IsNullOrEmpty(binary) && File.Exists(binary))
        {
            rep["binary_found"] = true;
            try
            {
                using var probe = File.Open(binary, FileMode.Open, FileAccess.Read);
                rep["binary_executable"] = true;
            }
            catch (Exception e)
            {
                errors.Add("binary not accessible: " + e.Message);
            }
            if ((bool)rep["binary_executable"]!)
            {
                if (HasCliShape(binary))
                    rep["cli_shape_ok"] = true;
                else
                    errors.Add("binary does not speak the managed CLI shape");
            }
        }
        else
        {
            errors.Add("binary not found: set it explicitly or via PATH (no silent download)");
        }
        try
        {
            var dir = Directory.CreateDirectory(
                Path.Combine(Path.GetTempPath(), "mx-doc-" + Path.GetRandomFileName())).FullName;
            try
            {
                var sockPath = Path.Combine(dir, "t.sock");
                using var sock = new Socket(AddressFamily.Unix, SocketType.Stream,
                    ProtocolType.Unspecified);
                sock.Bind(new UnixDomainSocketEndPoint(sockPath));
                rep["socket_dir_writable"] = true;
            }
            catch (Exception e)
            {
                errors.Add("unix socket probe failed: " + e.Message);
            }
            finally
            {
                try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
            }
        }
        catch (Exception e)
        {
            errors.Add("temp dir probe failed: " + e.Message);
        }
        return rep;
    }

    private static bool OnPath(string name)
    {
        var path = Environment.GetEnvironmentVariable("PATH") ?? "";
        foreach (var dir in path.Split(Path.PathSeparator))
        {
            try
            {
                if (File.Exists(Path.Combine(dir, name)))
                    return true;
            }
            catch { /* ignore */ }
        }
        return false;
    }
}

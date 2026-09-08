// Operator-side suites: mapping over a fake binary, bootstrap
// phases, doctor shape; live parts need MX_MATRIX_MANAGED + openssl.
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Matrix.Component;

static class OperatorSuites
{
    public static async Task<int> RunAsync()
    {
        int fail = 0;
        fail += await Case("mapping", () =>
        {
            var dir = Directory.CreateDirectory(
                Path.Combine(Path.GetTempPath(), "opfake-" + Path.GetRandomFileName())).FullName;
            try
            {
                var fake = Path.Combine(dir, "matrix-managed");
                File.WriteAllText(fake,
                    "#!/bin/sh\n" +
                    "if [ \"$1\" = \"request\" ]; then\n" +
                    "  case \"$7\" in\n" +
                    "    *sleep*) exec sleep 30;;\n" +
                    "    *badjson*) echo 'not json';;\n" +
                    "    *denied*) echo 'permission-denied: nope' >&2; exit 1;;\n" +
                    "    *) echo '{\"ok\":true}';;\n" +
                    "  esac\n" +
                    "else echo 'usage: matrix-managed serve <config>' >&2; exit 1\n" +
                    "fi\n");
                // Linux-only matrix (docs/ML1-MATRIX.md): no Windows branch ships.
#pragma warning disable CA1416
                File.SetUnixFileMode(fake,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
#pragma warning restore CA1416
                foreach (var n in new[] { "ca", "cert", "key" })
                    File.WriteAllText(Path.Combine(dir, n), "");
                var pki = new OperatorPki
                {
                    Ca = Path.Combine(dir, "ca"),
                    Cert = Path.Combine(dir, "cert"),
                    Key = Path.Combine(dir, "key"),
                };
                var c = Kernel.Attach(fake, "127.0.0.1:9", pki);
                var pong = c.Request(new Dictionary<string, object?> { ["action"] = "ping" });
                Check(pong["ok"].GetBoolean(), "ping");
                try
                {
                    c.Request(new Dictionary<string, object?> { ["action"] = "denied-op" });
                    Check(false, "denied-op succeeded");
                }
                catch (OperatorError e) { Check(e.Code == "permission-denied", "denial code " + e.Code); }
                try
                {
                    c.Request(new Dictionary<string, object?> { ["action"] = "badjson" });
                    Check(false, "badjson succeeded");
                }
                catch (OperatorError e) { Check(e.Code == "internal", "decode code " + e.Code); }
                try
                {
                    c.Request(new Dictionary<string, object?> { ["action"] = "sleep" },
                        TimeSpan.FromSeconds(1));
                    Check(false, "sleep succeeded");
                }
                catch (OperatorError e) { Check(e.Code == "outcome-unknown", "timeout code " + e.Code); }
                try
                {
                    c.Request(new Dictionary<string, object?> { ["no-action"] = true });
                    Check(false, "actionless succeeded");
                }
                catch (OperatorError e) { Check(e.Code == "invalid-message", "shape code " + e.Code); }
                c.Close();
                try
                {
                    c.Request(new Dictionary<string, object?> { ["action"] = "ping" });
                    Check(false, "closed client served");
                }
                catch (SdkError) { /* correct */ }
            }
            finally
            {
                try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
            }
            return Task.CompletedTask;
        });
        fail += await Case("bootstrap-phases", () =>
        {
            try
            {
                Kernel.Attach("/nonexistent/x", "127.0.0.1:1",
                    new OperatorPki { Ca = "a", Cert = "b", Key = "c" }).Request(
                    new Dictionary<string, object?> { ["action"] = "x" });
                Check(false, "bad attach served");
            }
            catch (BootstrapError e) { Check(e.Phase == "connect", "phase " + e.Phase); }
            try
            {
                Kernel.StartAsync("/nonexistent/matrix-managed",
                    new Dictionary<string, object?> { ["home"] = "/tmp/x" },
                    new OperatorPki()).Wait();
                Check(false, "missing binary started");
            }
            catch (AggregateException ae) when (ae.InnerException is BootstrapError be)
            {
                Check(be.Phase == "spawn", "phase " + be.Phase);
            }
            try
            {
                Kernel.StartAsync("/bin/true",
                    new Dictionary<string, object?> { ["components"] = Array.Empty<object>() },
                    new OperatorPki()).Wait();
                Check(false, "homeless config started");
            }
            catch (AggregateException ae) when (ae.InnerException is BootstrapError be)
            {
                Check(be.Phase == "config", "phase " + be.Phase);
            }
            return Task.CompletedTask;
        });
        fail += await Case("doctor-shape", () =>
        {
            var rep = Kernel.Doctor("/nonexistent/binary");
            foreach (var k in new[] { "dotnet", "binary", "binary_found", "cli_shape_ok",
                "openssl", "bwrap", "socket_dir_writable", "errors" })
                Check(rep.ContainsKey(k), "key " + k);
            var blob = JsonSerializer.Serialize(rep);
            Check(!blob.Replace("socket_dir_writable", "").Contains("lease"), "no secrets");
            var bin = LiveBinary();
            if (bin != null)
            {
                var rep2 = Kernel.Doctor(bin);
                Check((bool)rep2["binary_found"]! && (bool)rep2["cli_shape_ok"]!,
                    "binary recognized");
            }
            return Task.CompletedTask;
        });
        var liveBin = LiveBinary();
        var livePki = Environment.GetEnvironmentVariable("MX_DEV_PKI");
        if (liveBin != null && livePki != null && File.Exists(livePki))
        {
            fail += await Case("live-lifecycle", async () =>
            {
                var tmp = Directory.CreateDirectory(
                    Path.Combine(Path.GetTempPath(), "oplive-" + Path.GetRandomFileName())).FullName;
                try
                {
                    var pkiDir = Path.Combine(tmp, "pki");
                    var psi = new ProcessStartInfo("python3")
                    {
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                    };
                    psi.ArgumentList.Add(livePki ?? "");
                    psi.ArgumentList.Add(pkiDir);
                    psi.ArgumentList.Add("--server-name");
                    psi.ArgumentList.Add("localhost");
                    using var pk = Process.Start(psi) ??
                        throw new Exception("dev-pki spawn failed");
                    await pk.WaitForExitAsync().ConfigureAwait(false);
                    if (pk.ExitCode != 0)
                        throw new Exception("dev-pki failed");
                    var fp = Convert.ToHexString(
                        SHA256.HashData(await File.ReadAllBytesAsync(
                            Path.Combine(pkiDir, "client.der")))).ToLowerInvariant();
                    var cfg = new Dictionary<string, object?>
                    {
                        ["home"] = Path.Combine(tmp, "home"),
                        ["components"] = new object[]
                        {
                            new Dictionary<string, object?>
                            {
                                ["manifest"] = new Dictionary<string, object?>
                                {
                                    ["id"] = "echo",
                                    ["capabilities"] = new[] { "echo.msg@1" },
                                    ["reducer"] = "echo",
                                },
                                ["trusted"] = true,
                            },
                        },
                        ["grants"] = new Dictionary<string, object?>
                        {
                            [fp] = new Dictionary<string, object?>
                            {
                                ["components"] = new[] { "echo" },
                                ["capabilities"] = new[] { "echo.msg@1" },
                            },
                        },
                        ["tls"] = new Dictionary<string, object?>
                        {
                            ["listen"] = "127.0.0.1:0",
                            ["ca"] = Path.Combine(pkiDir, "ca.der"),
                            ["cert"] = Path.Combine(pkiDir, "server.der"),
                            ["key"] = Path.Combine(pkiDir, "server-key.der"),
                        },
                    };
                    var opki = new OperatorPki
                    {
                        Ca = Path.Combine(pkiDir, "ca.der"),
                        Cert = Path.Combine(pkiDir, "client.der"),
                        Key = Path.Combine(pkiDir, "client-key.der"),
                    };
                    await using var kernel = await Kernel.StartAsync(liveBin, cfg, opki)
                        .ConfigureAwait(false);
                    Check(kernel.Api.StartsWith("0.1."), "api " + kernel.Api);
                    var act = kernel.Client.Activate("echo", 20000);
                    var lease = act["lease"].GetString() ?? "";
                    var fence = act["fence"].GetString() ?? "";
                    var v = kernel.Client.Invoke(lease, fence, "op-live-1",
                        "echo.msg@1", new Dictionary<string, object?> { ["ping"] = 1 });
                    Check(v["ok"].GetBoolean(), "invoke");
                    var attached = Kernel.Attach(liveBin, kernel.Listen, opki);
                    var v2 = attached.Invoke(lease, fence, "op-live-2",
                        "echo.msg@1", new Dictionary<string, object?>());
                    Check(v2["ok"].GetBoolean(), "attached invoke");
                    attached.Close(); // attachment owns nothing
                    var v3 = kernel.Client.Invoke(lease, fence, "op-live-3",
                        "echo.msg@1", new Dictionary<string, object?>());
                    Check(v3["ok"].GetBoolean(), "post-attach invoke");
                    try
                    {
                        kernel.Client.Invoke("dead", "1", "op-x",
                            "echo.msg@1", new Dictionary<string, object?>());
                        Check(false, "dead lease served");
                    }
                    catch (OperatorError) { /* correct */ }
                    kernel.Client.Release(lease, fence);
                }
                finally
                {
                    try { Directory.Delete(tmp, recursive: true); } catch { /* best effort */ }
                }
            });
        }
        else
        {
            Console.WriteLine("skip live-lifecycle (needs MX_MATRIX_MANAGED + MX_DEV_PKI)");
        }
        return fail;
    }

    private static string? LiveBinary()
    {
        var env = Environment.GetEnvironmentVariable("MX_MATRIX_MANAGED");
        if (!string.IsNullOrEmpty(env) && File.Exists(env))
            return env;
        return null; // no fallback probing: set MX_MATRIX_MANAGED explicitly
    }


    private static async Task<int> Case(string name, Func<Task> body)
    {
        try
        {
            await body();
            Console.WriteLine($"ok {name}");
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine($"FAIL {name}: {e.Message}");
            return 1;
        }
    }

    private static void Check(bool cond, string what)
    {
        if (!cond)
            throw new Exception("check failed: " + what);
    }
}

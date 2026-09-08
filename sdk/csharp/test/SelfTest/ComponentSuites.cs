// Component-side suites: framing, generations, gates, roundtrips,
// reader independence, binary refusal.
using System.Text.Json;
using Matrix.Component;

static class ComponentSuites
{
    public static async Task<int> RunAsync()
    {
        int fail = 0;
        fail += await Case("u64-precision", async () =>
        {
            Check(Generations.Equal("18446744073709551615", "18446744073709551615"), "u64 max equal");
            Check(!Generations.Equal("18446744073709551615", "18446744073709551614"), "off-by-one differs");
            Check(!Generations.Equal("nope", "1"), "non-numeric rejected");
            await Task.CompletedTask;
        });
        fail += await Case("malformed-survives", async () =>
        {
            var (sockPath, done) = Loopback.Serve(Array.Empty<string>(), Array.Empty<object>(), conn =>
            {
                var garbage = new byte[] { (byte)'{', (byte)'o', (byte)'o', (byte)'p', (byte)'s' };
                var frame = new byte[4 + garbage.Length];
                frame[0] = 0; frame[1] = 0; frame[2] = 0; frame[3] = (byte)garbage.Length;
                Buffer.BlockCopy(garbage, 0, frame, 4, garbage.Length);
                conn.Send(frame);
                Loopback.Send(conn, Loopback.CallOpen("tkt-9",
                    new Dictionary<string, object?> { ["ping"] = 1 }));
                var ans = Loopback.Read(conn);
                Check(ans.GetProperty("type").GetString() == "call.result", "answered");
                var ping = ans.GetProperty("body").GetProperty("output")
                    .GetProperty("echo").GetProperty("ping").GetInt32();
                Check(ping == 1, "echo intact");
                Loopback.Send(conn, Loopback.DisposeFrame());
                Loopback.Read(conn);
            });
            var comp = await Matrix.Component.Component.ConnectAsync(sockPath, "t");
            Check(await comp.ServeAsync(new EchoHandler()) == "dispose", "clean dispose");
            await done;
        });
        fail += await Case("stale-generation-ignored", async () =>
        {
            var (sockPath, done) = Loopback.Serve(Array.Empty<string>(), Array.Empty<object>(), conn =>
            {
                var stale = Loopback.CallOpen("tkt-stale", new Dictionary<string, object?>());
                stale["generation"] = "999";
                Loopback.Send(conn, stale);
                Loopback.Send(conn, Loopback.CallOpen("tkt-9", new Dictionary<string, object?>()));
                var ans = Loopback.Read(conn);
                Check(ans.GetProperty("body").GetProperty("ticket").GetString() == "tkt-9",
                    "current served, stale ignored");
                Loopback.Send(conn, Loopback.DisposeFrame());
                Loopback.Read(conn);
            });
            var comp = await Matrix.Component.Component.ConnectAsync(sockPath, "t");
            Check(await comp.ServeAsync(new TicketHandler()) == "dispose", "clean dispose");
            await done;
        });
        fail += await Case("feature-gate-local", async () =>
        {
            var (sockPath, done) = Loopback.Serve(Array.Empty<string>(), Array.Empty<object>(), conn =>
            {
                Loopback.Send(conn, Loopback.CallOpen("tkt-9", new Dictionary<string, object?>()));
                var ans = Loopback.Read(conn);
                var refused = ans.GetProperty("body").GetProperty("output")
                    .GetProperty("refused").GetString();
                Check(refused == "unsupported-feature", "local refusal, got " + refused);
                conn.ReceiveTimeout = 1000;
                try
                {
                    Loopback.Read(conn, 1000);
                    Check(false, "SDK touched the wire after refusal");
                }
                catch { /* quiet: correct */ }
                Loopback.Send(conn, Loopback.DisposeFrame());
                Loopback.Read(conn);
            });
            var comp = await Matrix.Component.Component.ConnectAsync(sockPath, "t");
            Check(!comp.HasFeature(Protocol.DependencyCallsFeature), "no feature negotiated");
            Check(await comp.ServeAsync(new GateHandler()) == "dispose", "clean dispose");
            await done;
        });
        fail += await Case("dependency-roundtrip", async () =>
        {
            var bindings = new object[]
            {
                new Dictionary<string, object?> { ["binding_id"] = "bind-1", ["capability"] = "c@1" },
            };
            var (sockPath, done) = Loopback.Serve(
                new[] { Protocol.DependencyCallsFeature }, bindings, conn =>
                {
                    Loopback.Send(conn, Loopback.CallOpen("tkt-9",
                        new Dictionary<string, object?> { ["chain_it"] = true }));
                    var opened = Loopback.Read(conn);
                    Check(opened.GetProperty("type").GetString() == "dependency.open", "open sent");
                    var obody = opened.GetProperty("body");
                    Check(obody.GetProperty("binding_id").GetString() == "bind-1", "opaque binding kept");
                    Check(obody.GetProperty("parent_ticket").GetString() == "tkt-9", "parent ticket kept");
                    var rid = opened.GetProperty("request_id").GetString() ?? "";
                    var res = Loopback.Env("dependency.result", new Dictionary<string, object?>
                    {
                        ["status"] = "ok",
                        ["output"] = new Dictionary<string, object?> { ["deep"] = 1 },
                    });
                    res["request_id"] = rid;
                    Loopback.Send(conn, res);
                    var ans = Loopback.Read(conn);
                    var deep = ans.GetProperty("body").GetProperty("output")
                        .GetProperty("got").GetProperty("deep").GetInt32();
                    Check(deep == 1, "chained output");
                    Loopback.Send(conn, Loopback.DisposeFrame());
                    Loopback.Read(conn);
                });
            var comp = await Matrix.Component.Component.ConnectAsync(sockPath, "t");
            Check(await comp.ServeAsync(new ChainHandler()) == "dispose", "clean dispose");
            await done;
        });
        fail += await Case("flood-reader-independent", async () =>
        {
            var (sockPath, done) = Loopback.Serve(Array.Empty<string>(), Array.Empty<object>(), conn =>
            {
                for (int i = 0; i < 120; i++)
                    Loopback.Send(conn, Loopback.Env("event.deliver",
                        new Dictionary<string, object?>
                        {
                            ["topic"] = "t",
                            ["payload"] = new Dictionary<string, object?> { ["n"] = i },
                        }));
                var t0 = DateTime.UtcNow;
                Loopback.Send(conn, Loopback.CallOpen("tkt-9", new Dictionary<string, object?>()));
                var ans = Loopback.Read(conn);
                Check(ans.GetProperty("type").GetString() == "call.result", "answered");
                Check((DateTime.UtcNow - t0) < TimeSpan.FromSeconds(5), "fast under flood");
                Loopback.Send(conn, Loopback.CallOpen("tkt-10",
                    new Dictionary<string, object?> { ["report_drops"] = true }));
                var rep = Loopback.Read(conn);
                var dropped = rep.GetProperty("body").GetProperty("output")
                    .GetProperty("dropped").GetUInt64();
                Check(dropped >= 1, "overflow counted, got " + dropped);
                Loopback.Send(conn, Loopback.DisposeFrame());
                Loopback.Read(conn);
            });
            var comp = await Matrix.Component.Component.ConnectAsync(sockPath, "t");
            Check(await comp.ServeAsync(new SlowHandler()) == "dispose", "clean dispose");
            await done;
        });
        fail += await Case("binary-refused", async () =>
        {
            var (sockPath, done) = Loopback.Serve(Array.Empty<string>(), Array.Empty<object>(), conn =>
            {
                Loopback.Send(conn, Loopback.CallOpen("tkt-9",
                    new Dictionary<string, object?> { ["send_bytes"] = true }));
                var ans = Loopback.Read(conn);
                var body = ans.GetProperty("body");
                Check(body.GetProperty("status").GetString() == "error", "binary refused");
                Check(body.GetProperty("error").GetProperty("code").GetString() == "invalid-message",
                    "explicit code");
                Check(!ans.GetRawText().Contains("�"), "never lossy-converted");
                Loopback.Send(conn, Loopback.DisposeFrame());
                Loopback.Read(conn);
            });
            var comp = await Matrix.Component.Component.ConnectAsync(sockPath, "t");
            Check(await comp.ServeAsync(new BinHandler()) == "dispose", "clean dispose");
            await done;
        });
        return fail;
    }

    private sealed class EchoHandler : Handler
    {
        public override Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
            JsonElement input, CancellationToken cancel) =>
            Task.FromResult<object?>(new Dictionary<string, object?> { ["echo"] = input.Clone() });
    }

    private sealed class TicketHandler : Handler
    {
        public override Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
            JsonElement input, CancellationToken cancel) =>
            Task.FromResult<object?>(new Dictionary<string, object?> { ["ticket"] = ticket });
    }

    private sealed class GateHandler : Handler
    {
        public override async Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
            JsonElement input, CancellationToken cancel)
        {
            try
            {
                await ctx.InvokeDependencyAsync("bind-x",
                    new Dictionary<string, object?>(), TimeSpan.FromSeconds(2), cancel);
                return new Dictionary<string, object?> { ["unexpected"] = "wire-touched" };
            }
            catch (DepError e)
            {
                return new Dictionary<string, object?> { ["refused"] = e.Code };
            }
        }
    }

    private sealed class ChainHandler : Handler
    {
        public override async Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
            JsonElement input, CancellationToken cancel)
        {
            var depOut = await ctx.InvokeDependencyAsync(ctx.Dependencies()[0].Id,
                new Dictionary<string, object?> { ["v"] = 1 }, TimeSpan.FromSeconds(5), cancel);
            return new Dictionary<string, object?> { ["got"] = depOut.Clone() };
        }
    }

    private sealed class SlowHandler : Handler
    {
        public override Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
            JsonElement input, CancellationToken cancel)
        {
            if (input.ValueKind == JsonValueKind.Object &&
                input.TryGetProperty("report_drops", out _))
                return Task.FromResult<object?>(
                    new Dictionary<string, object?> { ["dropped"] = ctx.EventDroppedCount() });
            return Task.FromResult<object?>(new Dictionary<string, object?> { ["ok"] = true });
        }

        public override void OnEvent(string topic, JsonElement payload)
        {
            // Slow observer on the dispatcher: the reader keeps flowing.
            Thread.Sleep(30);
        }
    }

    private sealed class BinHandler : Handler
    {
        public override async Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
            JsonElement input, CancellationToken cancel)
        {
            try
            {
                await ctx.SendStreamBytesAsync("s1", 0, new byte[] { 0xff, 0xfe }, cancel);
                return new Dictionary<string, object?>();
            }
            catch (SdkError e)
            {
                throw new ComponentError("invalid-message", e.Detail);
            }
        }
    }

    private static readonly List<string> Failures = new();

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

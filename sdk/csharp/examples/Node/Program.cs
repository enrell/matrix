// Generic Matrix test node for C# (ML1 contract: docs/ML1-NODE.md).
//
// Usage: mx-node --matrix-sock <sock> --id <logical>
//        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
using System.Text.Json;
using Matrix.Component;

static class Args
{
    public static string? Get(string[] args, string key)
    {
        for (int i = 0; i + 1 < args.Length; i++)
            if (args[i] == key)
                return args[i + 1];
        return null;
    }
}

sealed class Node : Handler
{
    private readonly string _id;
    private readonly string? _eventLog;
    private readonly string? _streamLog;
    private readonly int _streamSlowMs;

    public Node(string id, string? eventLog, string? streamLog, int streamSlowMs)
    {
        _id = id;
        _eventLog = eventLog;
        _streamLog = streamLog;
        _streamSlowMs = streamSlowMs;
    }

    private static void AppendLine(string? path, string line)
    {
        if (path == null)
            return;
        try { File.AppendAllText(path, line + "\n"); }
        catch { /* log loss never fails the call */ }
    }

    public override void OnEvent(string topic, JsonElement payload) =>
        AppendLine(_eventLog, topic + "\t" + payload.GetRawText());

    public override void OnStream(string streamId, ulong seq, string payload)
    {
        if (_streamSlowMs > 0)
            Thread.Sleep(_streamSlowMs);
        AppendLine(_streamLog, $"{streamId}\t{seq}\t{payload.Length}");
    }

    private static async Task AbortableSleepAsync(int ms, CancellationToken cancel)
    {
        int slept = 0;
        while (slept < ms)
        {
            await Task.Delay(5, cancel).ConfigureAwait(false);
            slept += 5;
        }
    }

    private static bool Num(JsonElement el, string key, out double value)
    {
        value = 0;
        if (el.ValueKind != JsonValueKind.Object || !el.TryGetProperty(key, out var v))
            return false;
        if (v.ValueKind == JsonValueKind.Number && v.TryGetDouble(out value))
            return true;
        return false;
    }

    private static string Str(JsonElement el, string key)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(key, out var v) &&
            v.ValueKind == JsonValueKind.String)
            return v.GetString() ?? "";
        return "";
    }

    public override async Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
        JsonElement input, CancellationToken cancel)
    {
        if (input.ValueKind != JsonValueKind.Object)
            input = JsonDocument.Parse("{}").RootElement;
        if (Num(input, "sleep_ms", out var sleepN) && sleepN > 0)
        {
            try { await AbortableSleepAsync((int)sleepN, cancel).ConfigureAwait(false); }
            catch (OperationCanceledException) { throw new ComponentError("cancelled", "aborted"); }
        }
        var fail = Str(input, "fail");
        if (fail != "")
            throw new ComponentError(fail, "remote " + fail);
        if (Num(input, "amplify", out var amp))
        {
            int n = (int)Math.Min(Math.Max(amp, 0), 1 << 20);
            return new Dictionary<string, object?> { ["blob"] = new string('x', n), ["via"] = _id };
        }
        if (input.TryGetProperty("chain", out var chainEl) && chainEl.ValueKind == JsonValueKind.True)
        {
            var bindings = ctx.Dependencies();
            if (bindings.Count == 0)
                throw new ComponentError("dependency-unavailable", "no binding");
            JsonElement inner = default;
            if (input.TryGetProperty("input", out var inEl) && inEl.ValueKind == JsonValueKind.Object)
                inner = inEl;
            double timeoutMs = 5000;
            if (Num(input, "timeout_ms", out var tm))
                timeoutMs = tm;
            var output = await ctx.InvokeDependencyAsync(bindings[0].Id, inner,
                TimeSpan.FromMilliseconds(Math.Max(timeoutMs, 1)), cancel).ConfigureAwait(false);
            return new Dictionary<string, object?> { ["chained"] = output, ["via"] = _id };
        }
        if (input.TryGetProperty("acquire", out var acqEl) && acqEl.ValueKind == JsonValueKind.Object)
        {
            ulong? ms = null;
            if (acqEl.TryGetProperty("interval_ms", out var msEl) &&
                msEl.ValueKind == JsonValueKind.Number && msEl.TryGetUInt64(out var msv))
                ms = msv;
            var h = await ctx.AcquireResourceAsync(Str(acqEl, "kind"), Str(acqEl, "label"), ms, cancel)
                .ConfigureAwait(false);
            return new Dictionary<string, object?>
            {
                ["acquired"] = new Dictionary<string, object?> { ["handle"] = h.ToString() },
                ["via"] = _id,
            };
        }
        if (input.TryGetProperty("release", out var relEl))
        {
            ulong h = 0;
            if (relEl.ValueKind == JsonValueKind.Number)
                h = relEl.GetUInt64();
            else if (relEl.ValueKind == JsonValueKind.String && !ulong.TryParse(relEl.GetString(), out h))
                throw new ComponentError("invalid-message", "bad release");
            await ctx.ReleaseResourceAsync(h, cancel).ConfigureAwait(false);
            return new Dictionary<string, object?> { ["released"] = h.ToString(), ["via"] = _id };
        }
        if (input.TryGetProperty("stream_send", out var spec) && spec.ValueKind == JsonValueKind.Object)
        {
            var streamId = Str(spec, "stream_id");
            if (streamId == "")
                streamId = "s-test";
            long chunks = 0, nbytes = 0, slp = 0;
            if (spec.TryGetProperty("chunks", out var cEl) && cEl.ValueKind == JsonValueKind.Number)
                chunks = cEl.GetInt64();
            if (spec.TryGetProperty("chunk_bytes", out var bEl) && bEl.ValueKind == JsonValueKind.Number)
                nbytes = bEl.GetInt64();
            if (spec.TryGetProperty("sleep_ms", out var sEl) && sEl.ValueKind == JsonValueKind.Number)
                slp = sEl.GetInt64();
            chunks = Math.Min(Math.Max(chunks, 0), 256);
            nbytes = Math.Min(Math.Max(nbytes, 0), 4096);
            var payload = new string('x', (int)nbytes);
            long sent = 0;
            for (long seq = 0; seq < chunks; seq++)
            {
                cancel.ThrowIfCancellationRequested();
                try
                {
                    await ctx.SendStreamAsync(streamId, (ulong)seq, payload, cancel)
                        .ConfigureAwait(false);
                }
                catch (SdkError e)
                {
                    throw new ComponentError("stream-refused", e.Detail);
                }
                sent++;
                if (slp > 0)
                {
                    try { await AbortableSleepAsync((int)Math.Min(slp, 50), cancel).ConfigureAwait(false); }
                    catch (OperationCanceledException) { throw new ComponentError("cancelled", "aborted"); }
                }
            }
            return new Dictionary<string, object?> { ["stream_sent"] = sent, ["via"] = _id };
        }
        if (input.TryGetProperty("chain_with_streams", out var cspec) && cspec.ValueKind == JsonValueKind.Object)
        {
            // Concurrent chain + streams (M7 bidi legs): streams while
            // the child leg is in flight on this same session.
            return await ChainWithStreamsAsync(ctx, cspec, cancel).ConfigureAwait(false);
        }
        return new Dictionary<string, object?> { ["echo"] = input.Clone(), ["via"] = _id };
    }

    private static long SpecLong(JsonElement spec, string key, long dflt)
    {
        if (spec.TryGetProperty(key, out var el) && el.ValueKind == JsonValueKind.Number)
            return el.GetInt64();
        return dflt;
    }

    private async Task<object?> ChainWithStreamsAsync(CallCtx ctx, JsonElement spec,
        CancellationToken cancel)
    {
        var streamId = Str(spec, "stream_id");
        if (streamId == "")
            streamId = "s-bidi";
        long chunks = Math.Min(Math.Max(SpecLong(spec, "chunks", 0), 0), 32);
        long nbytes = Math.Min(Math.Max(SpecLong(spec, "chunk_bytes", 0), 0), 1024);
        long interval = Math.Min(Math.Max(SpecLong(spec, "interval_ms", 20), 0), 50);
        long prime = Math.Min(Math.Max(SpecLong(spec, "prime_ms", 50), 0), 1000);
        var payload = new string('x', (int)nbytes);
        long sent = 0;
        var streamer = Task.Run(async () =>
        {
            if (prime > 0)
                await Task.Delay((int)prime, CancellationToken.None).ConfigureAwait(false);
            for (long seq = 0; seq < chunks; seq++)
            {
                try
                {
                    await ctx.SendStreamAsync(streamId, (ulong)seq, payload, CancellationToken.None)
                        .ConfigureAwait(false);
                    sent++;
                }
                catch (SdkError) { break; }
                if (interval > 0)
                    await Task.Delay((int)interval, CancellationToken.None).ConfigureAwait(false);
            }
        }, CancellationToken.None);
        var bindings = ctx.Dependencies();
        if (bindings.Count == 0)
        {
            await streamer.ConfigureAwait(false);
            throw new ComponentError("dependency-unavailable", "no binding");
        }
        JsonElement inner = JsonDocument.Parse("{}").RootElement.Clone();
        if (spec.TryGetProperty("input", out var inEl) && inEl.ValueKind == JsonValueKind.Object)
            inner = inEl.Clone();
        var timeoutMs = Math.Max(SpecLong(spec, "timeout_ms", 8000), 1);
        JsonElement depOut;
        try
        {
            depOut = await ctx.InvokeDependencyAsync(bindings[0].Id, inner,
                TimeSpan.FromMilliseconds(timeoutMs), cancel).ConfigureAwait(false);
        }
        catch (DepError e)
        {
            await streamer.ConfigureAwait(false);
            throw new ComponentError(e.Code, e.Detail);
        }
        await streamer.ConfigureAwait(false);
        return new Dictionary<string, object?>
        {
            ["chained"] = depOut.Clone(),
            ["via"] = _id,
            ["stream_sent"] = sent,
        };
    }
}

static class Program
{
    static async Task<int> Main(string[] args)
    {
        var sock = Args.Get(args, "--matrix-sock");
        var id = Args.Get(args, "--id") ?? "dep-node";
        if (sock == null)
        {
            Console.Error.WriteLine("usage: mx-node --matrix-sock <sock> [--id <logical>] ...");
            return 2;
        }
        Matrix.Component.Component comp;
        try
        {
            comp = await Matrix.Component.Component.ConnectAsync(sock, id).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("connect: " + e.Message);
            return 2;
        }
        int slow = 0;
        int.TryParse(Args.Get(args, "--stream-slow-ms") ?? "0", out slow);
        string? reason;
        try
        {
            reason = await comp.ServeAsync(new Node(id, Args.Get(args, "--event-log"),
                Args.Get(args, "--stream-log"), slow)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("serve: " + e.Message);
            return 1;
        }
        if (reason != "dispose" && reason != "eof")
        {
            Console.Error.WriteLine("serve: " + reason);
            return 1;
        }
        return 0;
    }
}

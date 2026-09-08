// matrix.component/0.1 framing and component session for .NET (ML1).
//
// Mirrors the reference SDKs: handshake, registration, activation,
// call serving with cooperative cancellation (CancellationToken per
// call), dependency/resource roundtrips, bounded event/stream edge
// queue (64, drop-oldest, counted). Callbacks never run on the reader:
// slow observers throttle via host credit instead of stalling calls.
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace Matrix.Component;

public static class Protocol
{
    public const string Id = "matrix.component";
    public const string Version = "0.1";
    public const int DefaultMaxFrame = 1024 * 1024;
    public const int EventCap = 64;
    public const string DependencyCallsFeature = "dependency-calls/1";
}

/// <summary>Opaque activation binding handle (M6.1).</summary>
public sealed class DepBinding
{
    public string Id { get; init; } = "";
    public string Capability { get; init; } = "";
}

/// <summary>Generation comparisons without precision loss (u64 decimal strings).</summary>
public static class Generations
{
    /// <summary>True when both parse as u64 and are equal.</summary>
    public static bool Equal(string a, string b) => Wire.GenEqual(a, b);
}

internal static class Wire
{
    private static long _seq;

    public static string Fresh(string prefix) =>
        $"{prefix}-{System.Threading.Interlocked.Increment(ref _seq)}";

    public static bool GenEqual(string a, string b)
    {
        return ulong.TryParse(a?.Trim(), out var x) &&
               ulong.TryParse(b?.Trim(), out var y) && x == y;
    }

    public static async Task WriteFrameAsync(Socket sock, SemaphoreSlim gate,
        Dictionary<string, object?> msg, int maxFrame, CancellationToken ct)
    {
        var raw = JsonSerializer.SerializeToUtf8Bytes(msg);
        if (raw.Length > maxFrame)
            throw new SdkError("internal", "framing", "frame above max");
        var frame = new byte[4 + raw.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)raw.Length);
        Buffer.BlockCopy(raw, 0, frame, 4, raw.Length);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await sock.SendAsync(frame, SocketFlags.None, ct).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    public static async Task<JsonDocument?> ReadFrameAsync(Socket sock, int maxFrame,
        CancellationToken ct)
    {
        var hdr = await ReceiveExactAsync(sock, 4, ct).ConfigureAwait(false);
        if (hdr == null)
            return null; // EOF
        uint n = BinaryPrimitives.ReadUInt32BigEndian(hdr);
        if (n == 0 || n > (uint)maxFrame)
            throw new SdkError("internal", "framing", $"bad frame length {n}");
        var payload = await ReceiveExactAsync(sock, (int)n, ct).ConfigureAwait(false);
        if (payload == null)
            throw new SdkError("internal", "framing", "truncated frame");
        try
        {
            return JsonDocument.Parse(payload);
        }
        catch (JsonException)
        {
            return MalformedDoc.Value; // drop silently, keep session
        }
    }

    private static readonly Lazy<JsonDocument> MalformedDoc =
        new(() => JsonDocument.Parse("{\"__malformed\":true}"));

    private static async Task<byte[]?> ReceiveExactAsync(Socket sock, int count,
        CancellationToken ct)
    {
        var buf = new byte[count];
        int got = 0;
        while (got < count)
        {
            var seg = new ArraySegment<byte>(buf, got, count - got);
            int n;
            try
            {
                n = await sock.ReceiveAsync(seg, SocketFlags.None, ct).ConfigureAwait(false);
            }
            catch (SocketException e) when (e.SocketErrorCode == SocketError.OperationAborted ||
                                            e.SocketErrorCode == SocketError.Interrupted)
            {
                throw new OperationCanceledException(ct);
            }
            if (n == 0)
                return got == 0 ? null : throw new SdkError("internal", "framing", "truncated frame");
            got += n;
        }
        return buf;
    }
}

internal static class Json
{
    public static string Str(JsonElement el, string key)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(key, out var v) &&
            v.ValueKind == JsonValueKind.String)
            return v.GetString() ?? "";
        return "";
    }

    public static JsonElement Body(JsonElement env)
    {
        if (env.ValueKind == JsonValueKind.Object && env.TryGetProperty("body", out var b) &&
            b.ValueKind == JsonValueKind.Object)
            return b;
        return default;
    }
}

/// <summary>Per-call context: bound session, streams, dependencies.</summary>
public sealed class CallCtx
{
    private readonly Component _comp;
    public string Ticket { get; }

    internal CallCtx(Component comp, string ticket)
    {
        _comp = comp;
        Ticket = ticket;
    }

    public string SessionId => _comp.SessionId;

    /// <summary>Edge-queue drops (slow observers).</summary>
    public ulong EventDroppedCount() => _comp.DroppedCount();

    /// <summary>Queued stream chunks (credit signal).</summary>
    public int PendingStreamCount() => _comp.PendingStreams();

    /// <summary>Opaque handles of this activation.</summary>
    public IReadOnlyList<DepBinding> Dependencies() => _comp.Bindings;

    /// <summary>
    /// Sends one text chunk. Binary callers must encode first: byte[]
    /// is refused explicitly, never lossy-converted.
    /// </summary>
    public Task SendStreamAsync(string streamId, ulong seq, string payload,
        CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(streamId))
            throw new SdkError("invalid-message", "stream", "empty stream id");
        if (payload == null)
            throw new SdkError("invalid-message", "stream", "null payload");
        return _comp.SendEnvelopeAsync("stream.data", Wire.Fresh("m"), null,
            new Dictionary<string, object?>
            {
                ["stream_id"] = streamId,
                ["seq"] = seq.ToString(),
                ["payload"] = payload,
            }, ct);
    }

    /// <summary>Binary payloads refuse explicitly (no silent UTF-8 loss).</summary>
    public Task SendStreamBytesAsync(string streamId, ulong seq, byte[] _data,
        CancellationToken _ct = default)
    {
        throw new SdkError("invalid-message", "stream",
            "stream payloads are text; binary must be refused, never lossy-converted");
    }

    /// <summary>
    /// Invokes a dependency by opaque handle. Blocks until terminal,
    /// inheriting call cancellation. Without local negotiation refuses
    /// with unsupported-feature, wire untouched.
    /// </summary>
    public async Task<JsonElement> InvokeDependencyAsync(string binding, object? input,
        TimeSpan timeout, CancellationToken callCancel)
    {
        if (!_comp.HasFeature(Protocol.DependencyCallsFeature))
            throw new DepError("unsupported-feature", "dependency calls not negotiated");
        if (timeout <= TimeSpan.Zero)
            throw new DepError("invalid-message", "timeout must be positive");
        var rid = Wire.Fresh("r-dep");
        var waiter = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        _comp.AddDepWaiter(rid, waiter);
        try
        {
            await _comp.SendEnvelopeAsync("dependency.open", Wire.Fresh("m-dep"), rid,
                new Dictionary<string, object?>
                {
                    ["parent_ticket"] = Ticket,
                    ["binding_id"] = binding,
                    ["timeout_ms"] = (long)timeout.TotalMilliseconds,
                    ["input"] = input ?? new Dictionary<string, object?>(),
                }, callCancel).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not DepError)
        {
            _comp.RemoveDepWaiter(rid);
            throw new DepError("internal", "send: " + e.Message);
        }
        // Local deadline = request + transport slack; expiry cancels on wire.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(callCancel);
        linked.CancelAfter(timeout + TimeSpan.FromSeconds(10));
        try
        {
            return await waiter.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (callCancel.IsCancellationRequested)
        {
            await _comp.SendDepCancelAsync(rid).ConfigureAwait(false);
            throw new DepError("cancelled", "parent cancelled");
        }
        catch (OperationCanceledException)
        {
            await _comp.SendDepCancelAsync(rid).ConfigureAwait(false);
            throw new DepError("outcome-unknown", "sdk wait timeout");
        }
        finally
        {
            _comp.RemoveDepWaiter(rid);
        }
    }

    private async Task<Dictionary<string, object?>> ResourceRoundtripAsync(string operation,
        Dictionary<string, object?> fields, CancellationToken callCancel)
    {
        var rid = Wire.Fresh("r-res");
        var waiter = new TaskCompletionSource<Dictionary<string, object?>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        _comp.AddResWaiter(rid, waiter);
        try
        {
            var body = new Dictionary<string, object?> { ["operation_id"] = Wire.Fresh("op-res") };
            foreach (var kv in fields)
                body[kv.Key] = kv.Value;
            await _comp.SendEnvelopeAsync("resource." + operation, Wire.Fresh("m-res"), rid,
                body, callCancel).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not ResError)
        {
            _comp.RemoveResWaiter(rid);
            throw new ResError("internal", "send: " + e.Message);
        }
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(callCancel);
        linked.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            return await waiter.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (callCancel.IsCancellationRequested)
        {
            throw new ResError("cancelled", "parent cancelled");
        }
        catch (OperationCanceledException)
        {
            throw new ResError("outcome-unknown", "resource wait timeout");
        }
        finally
        {
            _comp.RemoveResWaiter(rid);
        }
    }

    /// <summary>Acquires an activation resource; returns the wire handle.</summary>
    public async Task<ulong> AcquireResourceAsync(string kind, string label, ulong? intervalMs,
        CancellationToken callCancel)
    {
        var fields = new Dictionary<string, object?> { ["kind"] = kind, ["label"] = label };
        if (intervalMs.HasValue)
            fields["interval_ms"] = intervalMs.Value;
        var extra = await ResourceRoundtripAsync("acquire", fields, callCancel).ConfigureAwait(false);
        if (extra.TryGetValue("handle", out var h) && h is string s &&
            ulong.TryParse(s, out var n))
            return n;
        if (extra.TryGetValue("handle", out var h2) && h2 is JsonElement je &&
            je.ValueKind == JsonValueKind.String && ulong.TryParse(je.GetString(), out var n2))
            return n2;
        throw new ResError("internal", "missing handle");
    }

    /// <summary>Releases a handle from AcquireResourceAsync.</summary>
    public async Task ReleaseResourceAsync(ulong handle, CancellationToken callCancel)
    {
        await ResourceRoundtripAsync("release",
            new Dictionary<string, object?> { ["handle"] = handle.ToString() }, callCancel)
            .ConfigureAwait(false);
    }
}

/// <summary>
/// Component logic. OnCallAsync runs off the reader with a
/// CancellationToken; OnCancel observes cancellation; OnEvent/OnStream
/// run on the dispatcher: observe fast, never block it.
/// </summary>
public abstract class Handler
{
    public virtual Task<object?> OnCallAsync(CallCtx ctx, string ticket, string cap,
        JsonElement input, CancellationToken cancel) =>
        throw new SdkError("internal", "handler", "OnCallAsync not implemented");

    public virtual void OnCancel(string ticket) { }

    public virtual void OnEvent(string topic, JsonElement payload) { }

    public virtual void OnStream(string streamId, ulong seq, string payload) { }
}

internal sealed class EvItem
{
    public bool IsStream;
    public string Topic = "";
    public JsonElement Payload;
    public string StreamId = "";
    public ulong Seq;
    public string Text = "";
}

/// <summary>Connected component: negotiated, activated session.</summary>
public sealed class Component
{
    private readonly Socket _sock;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly Dictionary<string, TaskCompletionSource<JsonElement>> _depWaiters = new();
    private readonly Dictionary<string, TaskCompletionSource<Dictionary<string, object?>>> _resWaiters = new();
    private readonly Dictionary<string, CancellationTokenSource> _calls = new();
    private readonly Queue<EvItem> _evQueue = new();
    private ulong _evDropped;

    public string SessionId { get; }
    public string InstanceId { get; }
    public string Generation { get; }
    public int MaxFrame { get; private set; }
    public IReadOnlyList<string> Features { get; }
    public IReadOnlyList<DepBinding> Bindings { get; }

    private Component(Socket sock, string session, string instance, string generation,
        int maxFrame, List<string> features, List<DepBinding> bindings)
    {
        _sock = sock;
        SessionId = session;
        InstanceId = instance;
        Generation = generation;
        MaxFrame = maxFrame;
        Features = features;
        Bindings = bindings;
    }

    public bool HasFeature(string f) => Features.Contains(f);

    internal ulong DroppedCount()
    {
        lock (_evQueue)
            return _evDropped;
    }

    internal int PendingStreams()
    {
        lock (_evQueue)
        {
            int n = 0;
            foreach (var it in _evQueue)
                if (it.IsStream)
                    n++;
            return n;
        }
    }

    internal void AddDepWaiter(string rid, TaskCompletionSource<JsonElement> w)
    {
        lock (_depWaiters)
            _depWaiters[rid] = w;
    }

    internal void RemoveDepWaiter(string rid)
    {
        lock (_depWaiters)
            _depWaiters.Remove(rid);
    }

    internal void AddResWaiter(string rid, TaskCompletionSource<Dictionary<string, object?>> w)
    {
        lock (_resWaiters)
            _resWaiters[rid] = w;
    }

    internal void RemoveResWaiter(string rid)
    {
        lock (_resWaiters)
            _resWaiters.Remove(rid);
    }

    private readonly SemaphoreSlim _evWake = new(0);

    private void Enqueue(EvItem it)
    {
        lock (_evQueue)
        {
            if (_evQueue.Count >= Protocol.EventCap)
            {
                _evQueue.Dequeue();
                _evDropped++;
            }
            _evQueue.Enqueue(it);
        }
        if (_evWake.CurrentCount == 0)
        {
            try { _evWake.Release(); } catch (SemaphoreFullException) { }
        }
    }

    internal Task SendEnvelopeAsync(string type, string messageId, string? requestId,
        Dictionary<string, object?> body, CancellationToken ct)
    {
        var msg = new Dictionary<string, object?>
        {
            ["protocol"] = Protocol.Id,
            ["version"] = Protocol.Version,
            ["type"] = type,
            ["message_id"] = messageId,
            ["session_id"] = SessionId,
            ["instance_id"] = InstanceId,
            ["generation"] = Generation,
            ["body"] = body,
        };
        if (requestId != null)
            msg["request_id"] = requestId;
        return Wire.WriteFrameAsync(_sock, _writeGate, msg, MaxFrame, ct);
    }

    internal Task SendDepCancelAsync(string target)
    {
        return SendEnvelopeAsync("dependency.cancel", Wire.Fresh("m-dep-cancel"),
            Wire.Fresh("r-dep-cancel"),
            new Dictionary<string, object?> { ["target_request_id"] = target },
            CancellationToken.None);
    }

    /// <summary>Connects, negotiates, registers, confirms activation.</summary>
    public static async Task<Component> ConnectAsync(string sockPath, string logical,
        CancellationToken ct = default)
    {
        var sock = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await sock.ConnectAsync(new UnixDomainSocketEndPoint(sockPath), ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            sock.Dispose();
            throw new SdkError("transport", "connect", e.Message);
        }
        var comp = new Component(sock, "", "", "", Protocol.DefaultMaxFrame,
            new List<string>(), new List<DepBinding>());
        async Task Send0(Dictionary<string, object?> m) =>
            await Wire.WriteFrameAsync(sock, comp._writeGate, m, comp.MaxFrame, ct).ConfigureAwait(false);
        try
        {
            await Send0(new Dictionary<string, object?>
            {
                ["protocol"] = Protocol.Id,
                ["version"] = Protocol.Version,
                ["type"] = "hello",
                ["message_id"] = "h1",
                ["body"] = new Dictionary<string, object?>
                {
                    ["launch_token"] = Environment.GetEnvironmentVariable("MATRIX_LAUNCH_TOKEN") ?? "",
                    ["versions"] = new[] { "0.1" },
                    ["max_frame"] = Protocol.DefaultMaxFrame,
                    ["client"] = "matrix-component-cs",
                    ["features"] = new[] { Protocol.DependencyCallsFeature },
                },
            }).ConfigureAwait(false);
            using var welcome = await ReadExpectAsync(sock, comp.MaxFrame, "welcome", ct)
                .ConfigureAwait(false) ?? throw new SdkError("transport", "connect", "no welcome");
            var wbody = Json.Body(welcome.RootElement);
            var session = Json.Str(welcome.RootElement, "session_id");
            comp = new Component(sock, session, "", "", Protocol.DefaultMaxFrame,
                new List<string>(), new List<DepBinding>());
            if (wbody.TryGetProperty("max_frame", out var mf) && mf.TryGetUInt64(out var mfu) && mfu > 0)
                comp.MaxFrame = (int)Math.Min(mfu, (ulong)Protocol.DefaultMaxFrame * 4);
            if (wbody.TryGetProperty("features", out var feats) && feats.ValueKind == JsonValueKind.Array)
                foreach (var f in feats.EnumerateArray())
                    if (f.ValueKind == JsonValueKind.String)
                        ((List<string>)comp.Features).Add(f.GetString() ?? "");
            await Send0(new Dictionary<string, object?>
            {
                ["protocol"] = Protocol.Id,
                ["version"] = Protocol.Version,
                ["type"] = "component.register",
                ["message_id"] = "reg1",
                ["session_id"] = comp.SessionId,
                ["body"] = new Dictionary<string, object?>
                {
                    ["manifest"] = new Dictionary<string, object?> { ["id"] = logical },
                },
            }).ConfigureAwait(false);
            using var reg = await ReadExpectAsync(sock, comp.MaxFrame, "registered", ct)
                .ConfigureAwait(false) ?? throw new SdkError("transport", "connect", "register rejected");
            comp = new Component(sock, comp.SessionId, Json.Str(reg.RootElement, "instance_id"),
                Json.Str(reg.RootElement, "generation"), comp.MaxFrame,
                (List<string>)comp.Features, new List<DepBinding>());
            using var act = await ReadExpectAsync(sock, comp.MaxFrame, "lifecycle.activate", ct)
                .ConfigureAwait(false) ?? throw new SdkError("transport", "connect", "no activate");
            var abody = Json.Body(act.RootElement);
            if (abody.TryGetProperty("dependency_bindings", out var arr) &&
                arr.ValueKind == JsonValueKind.Array)
                foreach (var b in arr.EnumerateArray())
                {
                    var id = Json.Str(b, "binding_id");
                    var cap = Json.Str(b, "capability");
                    if (id != "" && cap != "")
                        ((List<DepBinding>)comp.Bindings).Add(new DepBinding { Id = id, Capability = cap });
                }
            object? op = "op?";
            if (abody.TryGetProperty("operation_id", out var opEl))
                op = opEl.ValueKind == JsonValueKind.String ? opEl.GetString() : opEl.ToString();
            string? actRid = null;
            if (act.RootElement.TryGetProperty("request_id", out var ridEl) &&
                ridEl.ValueKind == JsonValueKind.String)
                actRid = ridEl.GetString();
            await comp.SendEnvelopeAsync("lifecycle.result", "lc1", actRid,
                new Dictionary<string, object?>
                {
                    ["operation_id"] = op,
                    ["status"] = "ok",
                    ["pending"] = Array.Empty<object>(),
                }, ct).ConfigureAwait(false);
            return comp;
        }
        catch
        {
            sock.Dispose();
            throw;
        }
    }

    private static async Task<JsonDocument?> ReadExpectAsync(Socket sock, int maxFrame,
        string want, CancellationToken ct)
    {
        using var doc = await Wire.ReadFrameAsync(sock, maxFrame, ct).ConfigureAwait(false);
        if (doc == null)
            return null;
        if (doc.RootElement.TryGetProperty("__malformed", out _))
            throw new SdkError("transport", "connect", "malformed handshake frame");
        if (Json.Str(doc.RootElement, "type") != want)
            throw new SdkError("transport", "connect",
                $"expected {want}, got {Json.Str(doc.RootElement, "type")}");
        // Detach from disposal: reparse the raw text for the caller.
        return JsonDocument.Parse(doc.RootElement.GetRawText());
    }

    private bool BoundOk(JsonElement env)
    {
        if (Json.Str(env, "session_id") != SessionId)
            return false;
        var inst = Json.Str(env, "instance_id");
        if (inst != "" && inst != InstanceId)
            return false;
        var gen = Json.Str(env, "generation");
        if (gen != "" && !Wire.GenEqual(gen, Generation))
            return false;
        return true;
    }

    private Task ReplyLifecycleAsync(JsonElement body, string? requestId)
    {
        object? op = "op?";
        if (body.ValueKind == JsonValueKind.Object && body.TryGetProperty("operation_id", out var opEl))
            op = opEl.ValueKind == JsonValueKind.String ? opEl.GetString() : opEl.ToString();
        return SendEnvelopeAsync("lifecycle.result", Wire.Fresh("m-lc"), requestId,
            new Dictionary<string, object?>
            {
                ["operation_id"] = op,
                ["status"] = "ok",
                ["pending"] = Array.Empty<object>(),
            }, CancellationToken.None);
    }

    /// <summary>
    /// Serves until EOF/error or dispose. Returns the exit reason
    /// ("dispose" on clean withdrawal). Callbacks never run on the reader.
    /// </summary>
    public async Task<string> ServeAsync(Handler handler, CancellationToken ct = default)
    {
        using var evStop = new CancellationTokenSource();
        var dispatcher = DispatcherAsync(handler, evStop.Token);
        try
        {
            for (; ; )
            {
                JsonDocument? doc;
                try
                {
                    doc = await Wire.ReadFrameAsync(_sock, MaxFrame, ct).ConfigureAwait(false);
                }
                catch (SdkError)
                {
                    return "eof";
                }
                if (doc == null)
                    return "eof";
                using (doc)
                {
                    var env = doc.RootElement;
                    if (env.TryGetProperty("__malformed", out _))
                        continue;
                    if (!BoundOk(env))
                        continue;
                    var body = Json.Body(env);
                    string? rid = null;
                    if (env.TryGetProperty("request_id", out var ridEl) &&
                        ridEl.ValueKind == JsonValueKind.String)
                        rid = ridEl.GetString();
                    var type = Json.Str(env, "type");
                    if (type == "lifecycle.prepare" || type == "lifecycle.activate" ||
                        type == "lifecycle.quiesce")
                    {
                        await ReplyLifecycleAsync(body, rid).ConfigureAwait(false);
                    }
                    else if (type == "lifecycle.dispose")
                    {
                        await ReplyLifecycleAsync(body, rid).ConfigureAwait(false);
                        return "dispose";
                    }
                    else if (type == "call.open")
                    {
                        var ticket = Json.Str(body, "ticket");
                        var cap = Json.Str(body, "capability");
                        var input = body.ValueKind == JsonValueKind.Object &&
                            body.TryGetProperty("input", out var inEl)
                            ? inEl.Clone() : default;
                        var callCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        lock (_calls)
                            _calls[ticket] = callCts;
                        var ctx = new CallCtx(this, ticket);
                        var openRid = rid;
                        _ = Task.Run(async () =>
                        {
                            object? result = null;
                            ComponentError? business = null;
                            SdkError? sdkErr = null;
                            try
                            {
                                result = await handler.OnCallAsync(ctx, ticket, cap, input, callCts.Token)
                                    .ConfigureAwait(false);
                            }
                            catch (ComponentError ce) { business = ce; }
                            catch (SdkError se) { sdkErr = se; }
                            catch (Exception e) { business = new ComponentError("internal", "handler: " + e.Message); }
                            finally
                            {
                                lock (_calls)
                                    _calls.Remove(ticket);
                                callCts.Dispose();
                            }
                            if (callCts.IsCancellationRequested)
                                return; // late after cancel: stay silent
                            Dictionary<string, object?> rbody;
                            if (business != null)
                                rbody = new Dictionary<string, object?>
                                {
                                    ["ticket"] = ticket,
                                    ["status"] = "error",
                                    ["error"] = new Dictionary<string, object?>
                                    {
                                        ["code"] = business.Code,
                                        ["message"] = business.Message,
                                    },
                                };
                            else if (sdkErr != null)
                                rbody = new Dictionary<string, object?>
                                {
                                    ["ticket"] = ticket,
                                    ["status"] = "error",
                                    ["error"] = new Dictionary<string, object?>
                                    {
                                        ["code"] = sdkErr.Code,
                                        ["message"] = sdkErr.Detail,
                                    },
                                };
                            else
                                rbody = new Dictionary<string, object?>
                                {
                                    ["ticket"] = ticket,
                                    ["status"] = "ok",
                                    ["output"] = result,
                                };
                            try
                            {
                                await SendEnvelopeAsync("call.result", Wire.Fresh("m-call"),
                                    openRid, rbody, CancellationToken.None).ConfigureAwait(false);
                            }
                            catch { /* transport gone */ }
                        }, CancellationToken.None);
                    }
                    else if (type == "call.cancel")
                    {
                        var ticket = Json.Str(body, "ticket");
                        lock (_calls)
                            if (_calls.TryGetValue(ticket, out var cts))
                                cts.Cancel();
                        try { handler.OnCancel(ticket); } catch { /* ignore */ }
                    }
                    else if (type == "dependency.result")
                    {
                        if (rid != null)
                        {
                            TaskCompletionSource<JsonElement>? w = null;
                            lock (_depWaiters)
                                if (_depWaiters.TryGetValue(rid, out w))
                                    _depWaiters.Remove(rid);
                            if (w != null)
                            {
                                if (Json.Str(body, "status") == "ok")
                                {
                                    var output = body.ValueKind == JsonValueKind.Object &&
                                        body.TryGetProperty("output", out var oEl)
                                        ? oEl.Clone() : default;
                                    w.TrySetResult(output);
                                }
                                else
                                {
                                    var e = body.ValueKind == JsonValueKind.Object &&
                                        body.TryGetProperty("error", out var eEl) ? eEl : default;
                                    var ecode = Json.Str(e, "code");
                                    var emsg = Json.Str(e, "message");
                                    w.TrySetException(new DepError(
                                        ecode != "" ? ecode : "internal",
                                        emsg != "" ? emsg : "remote error"));
                                }
                            }
                        }
                    }
                    else if (type == "resource.result")
                    {
                        if (rid != null)
                        {
                            TaskCompletionSource<Dictionary<string, object?>>? w = null;
                            lock (_resWaiters)
                                if (_resWaiters.TryGetValue(rid, out w))
                                    _resWaiters.Remove(rid);
                            if (w != null)
                            {
                                if (Json.Str(body, "status") == "ok")
                                {
                                    var extra = new Dictionary<string, object?>();
                                    if (body.ValueKind == JsonValueKind.Object)
                                        foreach (var p in body.EnumerateObject())
                                            if (p.Name != "operation_id" && p.Name != "status")
                                                extra[p.Name] = p.Value.ValueKind == JsonValueKind.String
                                                    ? p.Value.GetString() : (object?)p.Value.ToString();
                                    w.TrySetResult(extra);
                                }
                                else
                                {
                                    var rcode = Json.Str(body, "code");
                                    var rmsg = Json.Str(body, "message");
                                    w.TrySetException(new ResError(
                                        rcode != "" ? rcode : "internal",
                                        rmsg != "" ? rmsg : "remote error"));
                                }
                            }
                        }
                    }
                    else if (type == "event.deliver")
                    {
                        var topic = Json.Str(body, "topic");
                        if (topic != "")
                        {
                            var payload = body.ValueKind == JsonValueKind.Object &&
                                body.TryGetProperty("payload", out var pEl) ? pEl.Clone() : default;
                            Enqueue(new EvItem { Topic = topic, Payload = payload });
                        }
                    }
                    else if (type == "stream.data")
                    {
                        var sid = Json.Str(body, "stream_id");
                        var seqStr = Json.Str(body, "seq");
                        string payload = "";
                        if (body.ValueKind == JsonValueKind.Object &&
                            body.TryGetProperty("payload", out var pEl) &&
                            pEl.ValueKind == JsonValueKind.String)
                            payload = pEl.GetString() ?? "";
                        else if (body.ValueKind == JsonValueKind.Object &&
                            body.TryGetProperty("payload", out var pEl2) &&
                            pEl2.ValueKind != JsonValueKind.Null)
                        {
                            // Non-text chunk payloads are dropped at the edge
                            // (never lossy-converted); the leg lives host-side.
                            continue;
                        }
                        if (sid != "" && ulong.TryParse(seqStr, out var seq))
                            Enqueue(new EvItem { IsStream = true, StreamId = sid, Seq = seq, Text = payload });
                    }
                    // other types: ignored without dropping the session
                }
            }
        }
        finally
        {
            evStop.Cancel();
            try { await dispatcher.ConfigureAwait(false); } catch { /* drain done */ }
        }
    }

    private async Task DispatcherAsync(Handler handler, CancellationToken stop)
    {
        for (; ; )
        {
            try
            {
                await _evWake.WaitAsync(TimeSpan.FromMilliseconds(100), stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            for (; ; )
            {
                EvItem? it = null;
                lock (_evQueue)
                    if (_evQueue.Count > 0)
                        it = _evQueue.Dequeue();
                if (it == null)
                    break;
                try
                {
                    if (it.IsStream)
                        handler.OnStream(it.StreamId, it.Seq, it.Text);
                    else
                        handler.OnEvent(it.Topic, it.Payload);
                }
                catch { /* handler bugs never kill the session */ }
                if (stop.IsCancellationRequested)
                    return;
            }
        }
    }
}

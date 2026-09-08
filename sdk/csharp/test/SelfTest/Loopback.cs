// Loopback fake host for the C# SDK self-test (no packages).
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

static class Loopback
{
    public const int MaxFrame = 1024 * 1024;

    public static void Send(Socket conn, Dictionary<string, object?> msg)
    {
        var raw = JsonSerializer.SerializeToUtf8Bytes(msg);
        var frame = new byte[4 + raw.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)raw.Length);
        Buffer.BlockCopy(raw, 0, frame, 4, raw.Length);
        conn.Send(frame);
    }

    public static JsonElement Read(Socket conn, int timeoutMs = 10000)
    {
        conn.ReceiveTimeout = timeoutMs;
        var hdr = ReceiveExact(conn, 4);
        uint n = BinaryPrimitives.ReadUInt32BigEndian(hdr);
        if (n == 0 || n > MaxFrame)
            throw new Exception($"bad length {n}");
        var payload = ReceiveExact(conn, (int)n);
        return JsonDocument.Parse(payload).RootElement.Clone();
    }

    private static byte[] ReceiveExact(Socket conn, int count)
    {
        var buf = new byte[count];
        int got = 0;
        while (got < count)
        {
            int n = conn.Receive(new ArraySegment<byte>(buf, got, count - got));
            if (n == 0)
                throw new Exception("eof");
            got += n;
        }
        return buf;
    }

    public static Dictionary<string, object?> Env(string type, Dictionary<string, object?> body)
    {
        return new Dictionary<string, object?>
        {
            ["protocol"] = "matrix.component",
            ["version"] = "0.1",
            ["type"] = type,
            ["message_id"] = "m1",
            ["session_id"] = "s1",
            ["instance_id"] = "1",
            ["generation"] = "1",
            ["request_id"] = "r1",
            ["body"] = body,
        };
    }

    public static Dictionary<string, object?> CallOpen(string ticket, object? input)
    {
        return new Dictionary<string, object?>
        {
            ["protocol"] = "matrix.component",
            ["version"] = "0.1",
            ["type"] = "call.open",
            ["message_id"] = "m-" + ticket,
            ["session_id"] = "s1",
            ["instance_id"] = "1",
            ["generation"] = "1",
            ["request_id"] = "r-" + ticket,
            ["body"] = new Dictionary<string, object?>
            {
                ["ticket"] = ticket,
                ["capability"] = "c@1",
                ["input"] = input ?? new Dictionary<string, object?>(),
            },
        };
    }

    public static Dictionary<string, object?> DisposeFrame()
    {
        return new Dictionary<string, object?>
        {
            ["protocol"] = "matrix.component",
            ["version"] = "0.1",
            ["type"] = "lifecycle.dispose",
            ["message_id"] = "d1",
            ["session_id"] = "s1",
            ["instance_id"] = "1",
            ["generation"] = "1",
            ["request_id"] = "rd1",
            ["body"] = new Dictionary<string, object?> { ["operation_id"] = "op", ["deadline_ms"] = 100 },
        };
    }

    /// <summary>
    /// Serves one connection through the handshake, then runs script.
    /// Returns the socket path and a task completing with the script.
    /// </summary>
    public static (string, Task) Serve(string[] features, object[] bindings, Action<Socket> script)
    {
        var dir = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "csunits-" + Path.GetRandomFileName())).FullName;
        var sockPath = Path.Combine(dir, "t.sock");
        var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        srv.Bind(new UnixDomainSocketEndPoint(sockPath));
        srv.Listen(1);
        var done = Task.Run(() =>
        {
            try
            {
                using var conn = srv.Accept();
                var hello = Read(conn);
                if (hello.GetProperty("type").GetString() != "hello")
                    throw new Exception("expected hello");
                Send(conn, new Dictionary<string, object?>
                {
                    ["protocol"] = "matrix.component",
                    ["version"] = "0.1",
                    ["type"] = "welcome",
                    ["message_id"] = "h1",
                    ["session_id"] = "s1",
                    ["body"] = new Dictionary<string, object?>
                    {
                        ["version"] = "0.1",
                        ["max_frame"] = MaxFrame,
                        ["limits"] = new Dictionary<string, object?>(),
                        ["features"] = features,
                    },
                });
                var reg = Read(conn);
                if (reg.GetProperty("type").GetString() != "component.register")
                    throw new Exception("expected register");
                Send(conn, new Dictionary<string, object?>
                {
                    ["protocol"] = "matrix.component",
                    ["version"] = "0.1",
                    ["type"] = "registered",
                    ["message_id"] = "r",
                    ["session_id"] = "s1",
                    ["instance_id"] = "1",
                    ["generation"] = "1",
                    ["body"] = new Dictionary<string, object?> { ["logical"] = "t" },
                });
                var act = Env("lifecycle.activate", new Dictionary<string, object?>
                {
                    ["operation_id"] = "op",
                    ["manifest"] = new Dictionary<string, object?>(),
                    ["bindings"] = Array.Empty<object>(),
                    ["dependency_bindings"] = bindings,
                });
                act["request_id"] = "q";
                Send(conn, act);
                var lc = Read(conn);
                if (lc.GetProperty("type").GetString() != "lifecycle.result")
                    throw new Exception("expected lifecycle.result");
                script(conn);
            }
            finally
            {
                srv.Close();
                try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
            }
        });
        return (sockPath, done);
    }
}

require "spec"
require "../src/matrix-component"

# Shared loopback fake host for the Crystal SDK specs (stdlib only).
module Loopback
  MAX_FRAME = 1024 * 1024

  def self.send(sock : UNIXSocket, msg) : Nil
    raw = msg.to_json.to_slice
    hdr = Bytes.new(4)
    IO::ByteFormat::BigEndian.encode(raw.size.to_u32, hdr)
    sock.write(hdr)
    sock.write(raw)
    sock.flush
  end

  def self.read(sock : UNIXSocket, timeout : Time::Span = 10.seconds) : JSON::Any
    sock.read_timeout = timeout
    hdr = Bytes.new(4)
    sock.read_fully(hdr)
    n = IO::ByteFormat::BigEndian.decode(UInt32, hdr).to_i
    raise "bad length #{n}" if n == 0 || n > MAX_FRAME
    payload = Bytes.new(n)
    sock.read_fully(payload)
    JSON.parse(String.new(payload))
  end

  def self.env(type : String, body : Hash(String, JSON::Any)) : Hash(String, JSON::Any)
    {
      "protocol"   => JSON::Any.new("matrix.component"),
      "version"    => JSON::Any.new("0.1"),
      "type"       => JSON::Any.new(type),
      "message_id" => JSON::Any.new("m1"),
      "session_id" => JSON::Any.new("s1"),
      "instance_id" => JSON::Any.new("1"),
      "generation" => JSON::Any.new("1"),
      "request_id" => JSON::Any.new("r1"),
      "body"       => JSON::Any.new(body),
    }
  end

  def self.call_open(ticket : String, input : JSON::Any) : Hash(String, JSON::Any)
    {
      "protocol"   => JSON::Any.new("matrix.component"),
      "version"    => JSON::Any.new("0.1"),
      "type"       => JSON::Any.new("call.open"),
      "message_id" => JSON::Any.new("m-#{ticket}"),
      "session_id" => JSON::Any.new("s1"),
      "instance_id" => JSON::Any.new("1"),
      "generation" => JSON::Any.new("1"),
      "request_id" => JSON::Any.new("r-#{ticket}"),
      "body"       => JSON::Any.new({
        "ticket"     => JSON::Any.new(ticket),
        "capability" => JSON::Any.new("c@1"),
        "input"      => input,
      }),
    }
  end

  def self.dispose : Hash(String, JSON::Any)
    env("lifecycle.dispose", {
      "operation_id" => JSON::Any.new("op"),
      "deadline_ms"  => JSON::Any.new(100_i64),
    })
  end

  # Serves one connection through the handshake, then runs script.
  # Returns {sock_path, done_channel}.
  def self.serve(features : Array(String), bindings : Array(JSON::Any), &script : UNIXSocket -> Nil) : {String, Channel(Nil)}
    dir = File.join(Dir.tempdir, "crunits-#{Random::Secure.hex(4)}")
    Dir.mkdir_p(dir)
    sock_path = File.join(dir, "t.sock")
    srv = UNIXServer.new(sock_path)
    done = Channel(Nil).new(1)
    spawn do
      begin
        conn = srv.accept
        raise "expected hello" unless read(conn)["type"].as_s == "hello"
        send(conn, {
          "protocol"   => JSON::Any.new("matrix.component"),
          "version"    => JSON::Any.new("0.1"),
          "type"       => JSON::Any.new("welcome"),
          "message_id" => JSON::Any.new("h1"),
          "session_id" => JSON::Any.new("s1"),
          "body"       => JSON::Any.new({
            "version"   => JSON::Any.new("0.1"),
            "max_frame" => JSON::Any.new(MAX_FRAME.to_i64),
            "limits"    => JSON::Any.new({} of String => JSON::Any),
            "features"  => JSON::Any.new(features.map { |f| JSON::Any.new(f) }),
          }),
        })
        raise "expected register" unless read(conn)["type"].as_s == "component.register"
        send(conn, {
          "protocol"   => JSON::Any.new("matrix.component"),
          "version"    => JSON::Any.new("0.1"),
          "type"       => JSON::Any.new("registered"),
          "message_id" => JSON::Any.new("r"),
          "session_id" => JSON::Any.new("s1"),
          "instance_id" => JSON::Any.new("1"),
          "generation" => JSON::Any.new("1"),
          "body"       => JSON::Any.new({"logical" => JSON::Any.new("t")}),
        })
        act = env("lifecycle.activate", {
          "operation_id"       => JSON::Any.new("op"),
          "manifest"           => JSON::Any.new({} of String => JSON::Any),
          "bindings"           => JSON::Any.new([] of JSON::Any),
          "dependency_bindings" => JSON::Any.new(bindings),
        })
        act["request_id"] = JSON::Any.new("q")
        send(conn, act)
        raise "expected lifecycle.result" unless read(conn)["type"].as_s == "lifecycle.result"
        script.call(conn)
      ensure
        conn.try &.close rescue nil
        srv.close rescue nil
        FileUtils.rm_rf(dir) rescue nil
        done.send(nil)
      end
    end
    {sock_path, done}
  end
end

class EchoHandler < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    JSON::Any.new({"echo" => input})
  end
end

class TicketHandler < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    JSON::Any.new({"ticket" => JSON::Any.new(ticket)})
  end
end

class GateHandler < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    begin
      ctx.invoke_dependency("bind-x", JSON::Any.new({} of String => JSON::Any), 2.seconds)
      JSON::Any.new({"unexpected" => JSON::Any.new("wire-touched")})
    rescue ex : Matrix::DepError
      JSON::Any.new({"refused" => JSON::Any.new(ex.code)})
    end
  end
end

class ChainHandler < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    dep_out = ctx.invoke_dependency(ctx.dependencies[0].id,
      JSON::Any.new({"v" => JSON::Any.new(1_i64)}), 5.seconds)
    JSON::Any.new({"got" => dep_out})
  end
end

class SlowHandler < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    if input["report_drops"]?
      JSON::Any.new({"dropped" => JSON::Any.new(ctx.event_dropped_count.to_i64)})
    else
      JSON::Any.new({"ok" => JSON::Any.new(true)})
    end
  end

  def on_event(topic : String, payload : JSON::Any) : Nil
    sleep 30.milliseconds # slow observer yields; reader keeps flowing
  end
end

class BinHandler < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    begin
      ctx.send_stream("s1", 0_u64, "x".to_slice)
      JSON::Any.new({} of String => JSON::Any)
    rescue ex : Matrix::SdkError
      raise Matrix::BusinessError.new("invalid-message", ex.detail)
    end
  end
end

describe "u64 handling" do
  it "keeps full precision as decimal strings" do
    Matrix.gen_equal?("18446744073709551615", "18446744073709551615").should be_true
    Matrix.gen_equal?("18446744073709551615", "18446744073709551614").should be_false
    Matrix.gen_equal?("nope", "1").should be_false
  end
end

describe "framing robustness" do
  it "malformed frame drops silently, session survives" do
    sock_path, done = Loopback.serve([] of String, [] of JSON::Any) do |conn|
      garbage = "{oops".to_slice
      hdr = Bytes.new(4)
      IO::ByteFormat::BigEndian.encode(garbage.size.to_u32, hdr)
      conn.write(hdr)
      conn.write(garbage)
      conn.flush
      Loopback.send(conn, Loopback.call_open("tkt-9",
        JSON::Any.new({"ping" => JSON::Any.new(1_i64)})))
      ans = Loopback.read(conn)
      ans["type"].as_s.should eq "call.result"
      ans["body"]["output"]["echo"]["ping"].as_i.should eq 1
      Loopback.send(conn, Loopback.dispose)
      Loopback.read(conn)
    end
    comp = Matrix::Component.connect(sock_path, "t")
    comp.serve(EchoHandler.new).should eq "dispose"
    done.receive
  end

  it "stale generation ignored, current still served" do
    sock_path, done = Loopback.serve([] of String, [] of JSON::Any) do |conn|
      stale = Loopback.call_open("tkt-stale", JSON::Any.new({} of String => JSON::Any))
      stale["generation"] = JSON::Any.new("999")
      Loopback.send(conn, stale)
      Loopback.send(conn, Loopback.call_open("tkt-9", JSON::Any.new({} of String => JSON::Any)))
      ans = Loopback.read(conn)
      ans["body"]["ticket"].as_s.should eq "tkt-9"
      Loopback.send(conn, Loopback.dispose)
      Loopback.read(conn)
    end
    comp = Matrix::Component.connect(sock_path, "t")
    comp.serve(TicketHandler.new).should eq "dispose"
    done.receive
  end
end

describe "feature gate" do
  it "invoke without negotiation refuses locally, wire untouched" do
    sock_path, done = Loopback.serve([] of String, [] of JSON::Any) do |conn|
      Loopback.send(conn, Loopback.call_open("tkt-9", JSON::Any.new({} of String => JSON::Any)))
      ans = Loopback.read(conn)
      ans["body"]["output"]["refused"].as_s.should eq "unsupported-feature"
      begin
        Loopback.read(conn, 1.second)
        raise "SDK touched the wire after local refusal"
      rescue ex
        raise ex if ex.message == "SDK touched the wire after local refusal"
        # quiet: correct
      end
      Loopback.send(conn, Loopback.dispose)
      Loopback.read(conn)
    end
    comp = Matrix::Component.connect(sock_path, "t")
    comp.has_feature?(Matrix::DEPENDENCY_CALLS_FEATURE).should be_false
    comp.serve(GateHandler.new).should eq "dispose"
    done.receive
  end
end

describe "dependency roundtrip" do
  it "open/result correlate by request id" do
    bindings = [JSON::Any.new({
      "binding_id" => JSON::Any.new("bind-1"),
      "capability" => JSON::Any.new("c@1"),
    })]
    sock_path, done = Loopback.serve([Matrix::DEPENDENCY_CALLS_FEATURE], bindings) do |conn|
      Loopback.send(conn, Loopback.call_open("tkt-9",
        JSON::Any.new({"chain_it" => JSON::Any.new(true)})))
      opened = Loopback.read(conn)
      opened["type"].as_s.should eq "dependency.open"
      opened["body"]["binding_id"].as_s.should eq "bind-1"
      opened["body"]["parent_ticket"].as_s.should eq "tkt-9"
      res = Loopback.env("dependency.result", {
        "status" => JSON::Any.new("ok"),
        "output" => JSON::Any.new({"deep" => JSON::Any.new(1_i64)}),
      })
      res["request_id"] = JSON::Any.new(opened["request_id"].as_s)
      Loopback.send(conn, res)
      ans = Loopback.read(conn)
      ans["body"]["output"]["got"]["deep"].as_i.should eq 1
      Loopback.send(conn, Loopback.dispose)
      Loopback.read(conn)
    end
    comp = Matrix::Component.connect(sock_path, "t")
    comp.serve(ChainHandler.new).should eq "dispose"
    done.receive
  end
end

describe "reader independence" do
  it "slow on_event does not stall calls; flood drops are counted" do
    sock_path, done = Loopback.serve([] of String, [] of JSON::Any) do |conn|
      120.times do |i|
        Loopback.send(conn, Loopback.env("event.deliver", {
          "topic"   => JSON::Any.new("t"),
          "payload" => JSON::Any.new({"n" => JSON::Any.new(i.to_i64)}),
        }))
      end
      t0 = Time.instant
      Loopback.send(conn, Loopback.call_open("tkt-9", JSON::Any.new({} of String => JSON::Any)))
      ans = Loopback.read(conn)
      ans["type"].as_s.should eq "call.result"
      (Time.instant - t0).should be < 5.seconds
      Loopback.send(conn, Loopback.call_open("tkt-10",
        JSON::Any.new({"report_drops" => JSON::Any.new(true)})))
      rep = Loopback.read(conn)
      (rep["body"]["output"]["dropped"].as_i64 >= 1).should be_true
      Loopback.send(conn, Loopback.dispose)
      Loopback.read(conn)
    end
    comp = Matrix::Component.connect(sock_path, "t")
    comp.serve(SlowHandler.new).should eq "dispose"
    done.receive
  end
end

describe "binary refusal" do
  it "non-text stream payload refused, never converted" do
    sock_path, done = Loopback.serve([] of String, [] of JSON::Any) do |conn|
      Loopback.send(conn, Loopback.call_open("tkt-9",
        JSON::Any.new({"send_bytes" => JSON::Any.new(true)})))
      ans = Loopback.read(conn)
      ans["body"]["status"].as_s.should eq "error"
      ans["body"]["error"]["code"].as_s.should eq "invalid-message"
      ans.to_json.includes?("�").should be_false
      Loopback.send(conn, Loopback.dispose)
      Loopback.read(conn)
    end
    comp = Matrix::Component.connect(sock_path, "t")
    comp.serve(BinHandler.new).should eq "dispose"
    done.receive
  end
end

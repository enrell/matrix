# Generic Matrix test node for Crystal (ML1 contract: docs/ML1-NODE.md).
#
# Usage: mx-node --matrix-sock <sock> --id <logical>
#        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
require "../src/matrix-component"

def append_line(path : String?, line : String) : Nil
  return if path.nil?
  File.open(path, "a") { |f| f.puts(line) } rescue nil
end

class Node < Matrix::Handler
  def initialize(@id : String, @event_log : String?, @stream_log : String?,
                 @stream_slow_ms : Int32)
  end

  def on_event(topic : String, payload : JSON::Any) : Nil
    append_line(@event_log, "#{topic}\t#{payload.to_json}")
  end

  def on_stream(stream_id : String, seq : UInt64, payload : String) : Nil
    sleep (@stream_slow_ms.milliseconds) if @stream_slow_ms > 0
    append_line(@stream_log, "#{stream_id}\t#{seq}\t#{payload.size}")
  end

  private def abortable_sleep(ms : Int64, cancel : Atomic(Bool)) : Bool
    slept = 0_i64
    while slept < ms
      return true if cancel.get
      sleep 5.milliseconds
      slept += 5
    end
    false
  end

  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    input = JSON::Any.new({} of String => JSON::Any) unless input.as_h?
    if (sleep_ms = input["sleep_ms"]?.try(&.as_i64?)) && sleep_ms > 0
      if abortable_sleep(sleep_ms, cancel)
        raise Matrix::BusinessError.new("cancelled", "aborted")
      end
    end
    if (fail = input["fail"]?.try(&.as_s?)) && !fail.empty?
      raise Matrix::BusinessError.new(fail, "remote #{fail}")
    end
    if (amp = input["amplify"]?.try(&.as_i64?))
      n = {Math.max(amp, 0_i64), (1_i64 << 20)}.min.to_i
      return JSON::Any.new({
        "blob" => JSON::Any.new("x" * n),
        "via"  => JSON::Any.new(@id),
      })
    end
    if input["chain"]?.try(&.as_bool?) == true
      bindings = ctx.dependencies
      raise Matrix::BusinessError.new("dependency-unavailable", "no binding") if bindings.empty?
      inner = input["input"]? || JSON::Any.new({} of String => JSON::Any)
      timeout_ms = input["timeout_ms"]?.try(&.as_i64?) || 5000_i64
      begin
        dep_out = ctx.invoke_dependency(bindings[0].id, inner,
          {Math.max(timeout_ms, 1_i64), 3600000_i64}.min.milliseconds)
        return JSON::Any.new({
          "chained" => dep_out,
          "via"     => JSON::Any.new(@id),
        })
      rescue ex : Matrix::DepError
        raise Matrix::BusinessError.new(ex.code, ex.detail)
      end
    end
    if (acq = input["acquire"]?) && acq.as_h?
      ms = acq["interval_ms"]?.try(&.as_i64?).try &.to_u64
      begin
        h = ctx.acquire_resource(acq["kind"]?.try(&.as_s?) || "",
          acq["label"]?.try(&.as_s?) || "", ms)
        return JSON::Any.new({
          "acquired" => JSON::Any.new({"handle" => JSON::Any.new(h.to_s)}),
          "via"      => JSON::Any.new(@id),
        })
      rescue ex : Matrix::ResError
        raise Matrix::BusinessError.new(ex.code, ex.detail)
      end
    end
    if (rel = input["release"]?)
      h = rel.as_i64?.try(&.to_u64) || rel.as_s?.try(&.to_u64?) ||
        raise Matrix::BusinessError.new("invalid-message", "bad release")
      begin
        ctx.release_resource(h)
        return JSON::Any.new({
          "released" => JSON::Any.new(rel.as_s? || h.to_s),
          "via"      => JSON::Any.new(@id),
        })
      rescue ex : Matrix::ResError
        raise Matrix::BusinessError.new(ex.code, ex.detail)
      end
    end
    if (spec = input["stream_send"]?) && spec.as_h?
      stream_id = spec["stream_id"]?.try(&.as_s?) || "s-test"
      chunks = spec["chunks"]?.try(&.as_i64?) || 0_i64
      nbytes = spec["chunk_bytes"]?.try(&.as_i64?) || 0_i64
      slp = spec["sleep_ms"]?.try(&.as_i64?) || 0_i64
      chunks = {Math.max(chunks, 0_i64), 256_i64}.min
      nbytes = {Math.max(nbytes, 0_i64), 4096_i64}.min
      payload = "x" * nbytes.to_i
      sent = 0_i64
      chunks.times do |seq|
        if cancel.get
          raise Matrix::BusinessError.new("cancelled", "aborted")
        end
        begin
          ctx.send_stream(stream_id, seq.to_u64, payload)
        rescue ex : Matrix::SdkError
          raise Matrix::BusinessError.new("stream-refused", ex.detail)
        end
        sent += 1
        if slp > 0
          if abortable_sleep({slp, 50_i64}.min, cancel)
            raise Matrix::BusinessError.new("cancelled", "aborted")
          end
        end
      end
      return JSON::Any.new({
        "stream_sent" => JSON::Any.new(sent),
        "via"         => JSON::Any.new(@id),
      })
    end
    if (spec = input["chain_with_streams"]?) && spec.as_h?
      return chain_with_streams(ctx, spec, cancel)
    end
    JSON::Any.new({
      "echo" => input,
      "via"  => JSON::Any.new(@id),
    })
  end

  # Concurrent chain + streams (M7 bidi legs): streams while the child
  # leg is in flight on this same session.
  private def chain_with_streams(ctx : Matrix::CallCtx, spec : JSON::Any,
                                 cancel : Atomic(Bool)) : JSON::Any
    stream_id = spec["stream_id"]?.try(&.as_s?) || "s-bidi"
    chunks = spec["chunks"]?.try(&.as_i64?) || 0_i64
    nbytes = spec["chunk_bytes"]?.try(&.as_i64?) || 0_i64
    interval = spec["interval_ms"]?.try(&.as_i64?) || 20_i64
    prime = spec["prime_ms"]?.try(&.as_i64?) || 50_i64
    chunks = {Math.max(chunks, 0_i64), 32_i64}.min
    nbytes = {Math.max(nbytes, 0_i64), 1024_i64}.min
    interval = {Math.max(interval, 0_i64), 50_i64}.min
    prime = {Math.max(prime, 0_i64), 1000_i64}.min
    payload = "x" * nbytes.to_i
    sent = Atomic(Int64).new(0_i64)
    streamer_done = Channel(Nil).new(1)
    spawn do
      sleep prime.milliseconds if prime > 0
      chunks.times do |seq|
        begin
          ctx.send_stream(stream_id, seq.to_u64, payload)
          sent.add(1)
        rescue
          break
        end
        sleep interval.milliseconds if interval > 0
      end
      streamer_done.send(nil)
    end
    bindings = ctx.dependencies
    if bindings.empty?
      streamer_done.receive
      raise Matrix::BusinessError.new("dependency-unavailable", "no binding")
    end
    inner = spec["input"]? || JSON::Any.new({} of String => JSON::Any)
    timeout_ms = spec["timeout_ms"]?.try(&.as_i64?) || 8000_i64
    begin
      dep_out = ctx.invoke_dependency(bindings[0].id, inner,
        {Math.max(timeout_ms, 1_i64), 3600000_i64}.min.milliseconds)
    rescue ex : Matrix::DepError
      streamer_done.receive
      raise Matrix::BusinessError.new(ex.code, ex.detail)
    end
    streamer_done.receive
    JSON::Any.new({
      "chained"     => dep_out,
      "via"         => JSON::Any.new(@id),
      "stream_sent" => JSON::Any.new(sent.get),
    })
  end
end

sock = nil
id = "dep-node"
event_log = nil
stream_log = nil
stream_slow_ms = 0
i = 0
while i < ARGV.size
  case ARGV[i]
  when "--matrix-sock"   then sock = ARGV[i + 1]?; i += 2
  when "--id"            then id = ARGV[i + 1]? || id; i += 2
  when "--event-log"     then event_log = ARGV[i + 1]?; i += 2
  when "--stream-log"    then stream_log = ARGV[i + 1]?; i += 2
  when "--stream-slow-ms" then stream_slow_ms = (ARGV[i + 1]?.try(&.to_i?) || 0); i += 2
  else                        i += 1
  end
end
if sock.nil?
  STDERR.puts "usage: mx-node --matrix-sock <sock> [--id <logical>] ..."
  exit 2
end
begin
  comp = Matrix::Component.connect(sock.not_nil!, id)
rescue ex
  STDERR.puts "connect: #{ex.message}"
  exit 2
end
reason = comp.serve(Node.new(id, event_log, stream_log, stream_slow_ms))
exit(reason == "dispose" || reason == "eof" ? 0 : 1)

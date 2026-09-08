# Matrix component SDK for Crystal (ML1, stdlib only).
#
# Speaks `matrix.component/0.1` with the local host: handshake,
# registration, activation, and call serving with cooperative
# cancellation. Mirrors the reference SDKs (Rust `matrix-component`,
# Python `matrix_component`): same observable behavior.
#
# Concurrency (epic table: fibers, channels, scoped blocks): the read
# loop never runs handler code — each call gets a fiber bound to a
# cooperative cancel flag, events/streams go to a dispatcher fiber over
# a bounded queue (64, drop-oldest, counted). Blocking dependency
# calls poll in 50 ms slices so cancel/deadline preempt the wait;
# deadlines include transport slack and never retry.
require "socket"
require "file_utils"
require "json"

module Matrix
  PROTOCOL_ID      = "matrix.component"
  PROTOCOL_VERSION = "0.1"
  DEFAULT_MAX_FRAME = 1024 * 1024
  EVENT_CAP         = 64
  DEPENDENCY_CALLS_FEATURE = "dependency-calls/1"

  # Structured error: stable code, phase, safe message.
  class SdkError < Exception
    getter code : String
    getter phase : String
    getter detail : String

    def initialize(@code : String, @phase : String, @detail : String)
      super("#{@code} [#{@phase}]: #{@detail}")
    end
  end

  # Dependency-call error (wire code, no reinterpretation).
  class DepError < SdkError
    def initialize(code : String, message : String)
      super(code, "dependency", message)
    end
  end

  # Activation resource error.
  class ResError < SdkError
    def initialize(code : String, message : String)
      super(code, "resource", message)
    end
  end

  # Business error raised by handlers: answered with this code/message.
  class BusinessError < Exception
    getter code : String

    def initialize(@code : String, message : String)
      super(message)
    end
  end

  # u64 comparisons without precision loss (decimal strings on the wire).
  def self.gen_equal?(a : String, b : String) : Bool
    x = a.strip.to_u64?
    y = b.strip.to_u64?
    !x.nil? && !y.nil? && x == y
  end

  # Opaque activation binding handle (M6.1).
  struct DepBinding
    getter id : String
    getter capability : String

    def initialize(@id : String, @capability : String)
    end
  end

  private SEQ = Atomic(Int64).new(1_i64)

  def self.fresh(prefix : String) : String
    "#{prefix}-#{SEQ.add(1)}"
  end

  # Length-prefixed JSON framing over a Unix socket. Frames are only
  # consumed when a reader waits: coalesced arrivals stay buffered for
  # the next read instead of being dropped.
  private alias FrameResult = JSON::Any? | Exception

  private class Framing
    @buf : IO::Memory
    @waiters : Deque(Channel(FrameResult))
    @ended : Bool
    @failed : Exception?
    @mu : Mutex

    def initialize(@sock : UNIXSocket, @max_frame : Int32)
      @buf = IO::Memory.new
      @waiters = Deque(Channel(FrameResult)).new
      @ended = false
      @failed = nil
      @mu = Mutex.new
    end

    property max_frame : Int32

    def feed(data : Bytes) : Nil
      @mu.synchronize do
        @buf.write(data)
        pump
      end
    end

    def fail(ex : Exception) : Nil
      @mu.synchronize do
        return if @failed
        @failed = ex
        @waiters.each &.send(ex)
        @waiters.clear
      end
    end

    def finish : Nil
      @mu.synchronize do
        @ended = true
        @waiters.each &.send(nil)
        @waiters.clear
      end
    end

    private def pump : Nil
      while @waiters.size > 0
        raw = @buf.to_slice
        break if raw.size < 4
        n = IO::ByteFormat::BigEndian.decode(UInt32, raw[0, 4]).to_i
        if n == 0 || n > @max_frame
          failed = SdkError.new("internal", "framing", "bad frame length #{n}")
          @failed = failed
          @waiters.each &.send(failed)
          @waiters.clear
          return
        end
        break if raw.size < 4 + n
        payload = raw[4, n].dup
        remaining = raw[(4 + n)..].dup
        @buf.clear
        @buf.write(remaining)
        msg : JSON::Any? = begin
          JSON.parse(String.new(payload))
        rescue
          JSON.parse(%({"__malformed":true}))
        end
        @waiters.shift.send(msg)
      end
    end

    def read : JSON::Any?
      ch = Channel(FrameResult).new(1)
      w = @mu.synchronize do
        if f = @failed
          raise f
        end
        if @ended
          nil
        else
          @waiters << ch
          pump
          ch
        end
      end
      return nil if w.nil?
      msg = w.receive
      raise msg if msg.is_a?(Exception)
      msg.as(JSON::Any?)
    end

    def write(obj) : Nil
      raw = obj.to_json.to_slice
      raise SdkError.new("internal", "framing", "frame above max") if raw.size > @max_frame
      hdr = Bytes.new(4)
      IO::ByteFormat::BigEndian.encode(raw.size.to_u32, hdr)
      @sock.write(hdr)
      @sock.write(raw)
      @sock.flush
    end
  end

  # Per-call context: bound session, streams, dependencies.
  class CallCtx
    getter ticket : String

    def initialize(@comp : Component, @ticket : String, @cancel : Atomic(Bool))
    end

    def session_id : String
      @comp.session_id
    end

    def cancelled? : Bool
      @cancel.get
    end

    # Edge-queue drops (slow observers).
    def event_dropped_count : UInt64
      @comp.dropped_count
    end

    # Queued stream chunks (credit signal).
    def pending_stream_count : Int32
      @comp.pending_streams
    end

    # Opaque handles of this activation.
    def dependencies : Array(DepBinding)
      @comp.bindings.dup
    end

    # Sends one text chunk. Binary callers must encode first: Bytes is
    # refused explicitly, never lossy-converted.
    def send_stream(stream_id : String, seq : UInt64, payload : String) : Nil
      raise SdkError.new("invalid-message", "stream", "empty stream id") if stream_id.empty?
      @comp.send_envelope("stream.data", Matrix.fresh("m"), nil, {
        "stream_id" => JSON::Any.new(stream_id),
        "seq"       => JSON::Any.new(seq.to_s),
        "payload"   => JSON::Any.new(payload),
      })
    end

    def send_stream(stream_id : String, seq : UInt64, payload : Bytes) : Nil
      raise SdkError.new("invalid-message", "stream",
        "stream payloads are text; binary must be refused, never lossy-converted")
    end

    # Invokes a dependency by opaque handle. Blocks (cooperatively)
    # until terminal, inheriting call cancellation. Without local
    # negotiation refuses with unsupported-feature, wire untouched.
    def invoke_dependency(binding : String, input : JSON::Any,
                          timeout : Time::Span) : JSON::Any
      comp = @comp
      unless comp.has_feature?(DEPENDENCY_CALLS_FEATURE)
        raise DepError.new("unsupported-feature", "dependency calls not negotiated")
      end
      raise DepError.new("invalid-message", "timeout must be positive") if timeout <= Time::Span.zero
      rid = Matrix.fresh("r-dep")
      ch = Channel(DepResult).new(1)
      comp.add_dep_waiter(rid, ch)
      begin
        comp.send_envelope("dependency.open", Matrix.fresh("m-dep"), rid, {
          "parent_ticket" => JSON::Any.new(@ticket),
          "binding_id"    => JSON::Any.new(binding),
          "timeout_ms"    => JSON::Any.new(timeout.total_milliseconds.to_i64),
          "input"         => input,
        })
      rescue ex
        comp.remove_dep_waiter(rid)
        raise DepError.new("internal", "send: #{ex.message}")
      end
      # Local deadline = request + transport slack; expiry cancels on wire.
      deadline = Time.instant + timeout + 10.seconds
      loop do
        select
        when result = ch.receive
          comp.remove_dep_waiter(rid)
          case result
          when JSON::Any then return result
          when DepError  then raise result
          else                raise DepError.new("internal", "bad waiter result")
          end
        when timeout(50.milliseconds)
          if @cancel.get
            comp.remove_dep_waiter(rid)
            comp.send_dep_cancel(rid)
            # Drain a racing terminal without blocking (waiter is gone).
            select
            when ch.receive then nil
            else                 nil
            end
            raise DepError.new("cancelled", "parent cancelled")
          end
          if Time.instant >= deadline
            comp.remove_dep_waiter(rid)
            comp.send_dep_cancel(rid)
            select
            when ch.receive then nil
            else                 nil
            end
            raise DepError.new("outcome-unknown", "sdk wait timeout")
          end
        end
      end
    end

    private def resource_roundtrip(operation : String, fields : Hash(String, JSON::Any)) : Hash(String, JSON::Any)
      comp = @comp
      rid = Matrix.fresh("r-res")
      ch = Channel(ResResult).new(1)
      comp.add_res_waiter(rid, ch)
      body = {"operation_id" => JSON::Any.new(Matrix.fresh("op-res"))}
      fields.each { |k, v| body[k] = v }
      begin
        comp.send_envelope("resource.#{operation}", Matrix.fresh("m-res"), rid, body)
      rescue ex
        comp.remove_res_waiter(rid)
        raise ResError.new("internal", "send: #{ex.message}")
      end
      deadline = Time.instant + 10.seconds
      loop do
        select
        when result = ch.receive
          comp.remove_res_waiter(rid)
          case result
          when Hash(String, JSON::Any) then return result
          when ResError                 then raise result
          else                               raise ResError.new("internal", "bad waiter result")
          end
        when timeout(50.milliseconds)
          if @cancel.get
            comp.remove_res_waiter(rid)
            raise ResError.new("cancelled", "parent cancelled")
          end
          if Time.instant >= deadline
            comp.remove_res_waiter(rid)
            raise ResError.new("outcome-unknown", "resource wait timeout")
          end
        end
      end
    end

    # Acquires an activation resource (cap/sub/timer/task).
    def acquire_resource(kind : String, label : String,
                         interval_ms : UInt64? = nil) : UInt64
      fields = {
        "kind"  => JSON::Any.new(kind),
        "label" => JSON::Any.new(label),
      }
      fields["interval_ms"] = JSON::Any.new(interval_ms.to_s) if interval_ms
      extra = resource_roundtrip("acquire", fields)
      h = extra["handle"]?.try &.as_s?
      n = h.try &.to_u64?
      raise ResError.new("internal", "missing handle") if n.nil?
      n
    end

    # Releases a handle from `acquire_resource`.
    def release_resource(handle : UInt64) : Nil
      resource_roundtrip("release", {"handle" => JSON::Any.new(handle.to_s)})
    end
  end

  private alias DepResult = JSON::Any | DepError
  private alias ResResult = Hash(String, JSON::Any) | ResError

  # Component logic. `on_call` runs on its own fiber with a cooperative
  # cancel flag; `on_cancel` observes cancellation; `on_event` and
  # `on_stream` run on the dispatcher fiber: observe fast, never block.
  abstract class Handler
    abstract def on_call(ctx : CallCtx, ticket : String, cap : String,
                         input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any

    def on_cancel(ticket : String) : Nil
    end

    def on_event(topic : String, payload : JSON::Any) : Nil
    end

    def on_stream(stream_id : String, seq : UInt64, payload : String) : Nil
    end
  end

  private struct EvItem
    property kind : Symbol
    property topic : String
    property payload : JSON::Any
    property stream_id : String
    property seq : UInt64
    property text : String

    def initialize(@kind : Symbol, @topic : String, @payload : JSON::Any,
                   @stream_id : String, @seq : UInt64, @text : String)
    end
  end

  # Connected component: negotiated, activated session.
  class Component
    getter session_id : String
    getter instance_id : String
    getter generation : String
    getter bindings : Array(DepBinding)

    @framing : Framing
    @features : Array(String)
    @write_mu : Mutex
    @dep_mu : Mutex
    @dep_waiters : Hash(String, Channel(DepResult))
    @res_mu : Mutex
    @res_waiters : Hash(String, Channel(ResResult))
    @calls_mu : Mutex
    @calls : Hash(String, Atomic(Bool))
    @ev_mu : Mutex
    @ev_queue : Deque(EvItem)
    @ev_dropped : Atomic(UInt64)
    @ev_wake : Channel(Nil)

    def initialize(@sock : UNIXSocket, @framing : Framing,
                   @session_id : String, @instance_id : String,
                   @generation : String, @features : Array(String),
                   @bindings : Array(DepBinding))
      @write_mu = Mutex.new
      @dep_mu = Mutex.new
      @dep_waiters = Hash(String, Channel(DepResult)).new
      @res_mu = Mutex.new
      @res_waiters = Hash(String, Channel(ResResult)).new
      @calls_mu = Mutex.new
      @calls = Hash(String, Atomic(Bool)).new
      @ev_mu = Mutex.new
      @ev_queue = Deque(EvItem).new
      @ev_dropped = Atomic(UInt64).new(0_u64)
      @ev_wake = Channel(Nil).new(1)
    end

    def has_feature?(f : String) : Bool
      @features.includes?(f)
    end

    def dropped_count : UInt64
      @ev_dropped.get
    end

    def pending_streams : Int32
      @ev_mu.synchronize { @ev_queue.count(&.kind.==(:stream)) }
    end

    def add_dep_waiter(rid : String, ch : Channel(DepResult)) : Nil
      @dep_mu.synchronize { @dep_waiters[rid] = ch }
    end

    def remove_dep_waiter(rid : String) : Nil
      @dep_mu.synchronize { @dep_waiters.delete(rid) }
    end

    def add_res_waiter(rid : String, ch : Channel(ResResult)) : Nil
      @res_mu.synchronize { @res_waiters[rid] = ch }
    end

    def remove_res_waiter(rid : String) : Nil
      @res_mu.synchronize { @res_waiters.delete(rid) }
    end

    def send_envelope(type : String, message_id : String, request_id : String?,
                      body : Hash(String, JSON::Any)) : Nil
      msg = {
        "protocol"   => JSON::Any.new(PROTOCOL_ID),
        "version"    => JSON::Any.new(PROTOCOL_VERSION),
        "type"       => JSON::Any.new(type),
        "message_id" => JSON::Any.new(message_id),
        "session_id" => JSON::Any.new(@session_id),
        "instance_id" => JSON::Any.new(@instance_id),
        "generation" => JSON::Any.new(@generation),
        "body"       => JSON::Any.new(body),
      }
      msg["request_id"] = JSON::Any.new(request_id) if request_id
      @write_mu.synchronize { @framing.write(msg) }
    end

    def send_dep_cancel(target : String) : Nil
      send_envelope("dependency.cancel", Matrix.fresh("m-dep-cancel"),
        Matrix.fresh("r-dep-cancel"), {"target_request_id" => JSON::Any.new(target)})
    rescue
      # fire-and-forget
    end

    # Connects, negotiates, registers `logical`, confirms activation.
    private def self.spawn_pump(sock : UNIXSocket, framing : Framing) : Nil
      spawn do
        buf = Bytes.new(65536)
        loop do
          n = begin
            sock.read(buf)
          rescue
            0
          end
          break if n == 0
          framing.feed(buf[0, n])
        end
        framing.finish
      end
    end

    def self.connect(sock_path : String, logical : String) : Component
      sock = UNIXSocket.new(sock_path)
      sock.read_timeout = 30.seconds
      framing = Framing.new(sock, DEFAULT_MAX_FRAME)
      spawn_pump(sock, framing)
      send0 = ->(msg : Hash(String, JSON::Any)) { framing.write(msg) }
      send0.call({
        "protocol"   => JSON::Any.new(PROTOCOL_ID),
        "version"    => JSON::Any.new(PROTOCOL_VERSION),
        "type"       => JSON::Any.new("hello"),
        "message_id" => JSON::Any.new("h1"),
        "body"       => JSON::Any.new({
          "launch_token" => JSON::Any.new(ENV.fetch("MATRIX_LAUNCH_TOKEN", "")),
          "versions"     => JSON::Any.new([JSON::Any.new("0.1")]),
          "max_frame"    => JSON::Any.new(DEFAULT_MAX_FRAME.to_i64),
          "client"       => JSON::Any.new("matrix-component-cr"),
          "features"     => JSON::Any.new([JSON::Any.new(DEPENDENCY_CALLS_FEATURE)]),
        }),
      })
      welcome = framing.read
      raise "expected welcome, got #{welcome.inspect}" unless welcome.try(&.["type"]?.try(&.as_s?)) == "welcome"
      w = welcome.not_nil!
      session = w["session_id"].as_s
      max_frame = w["body"]["max_frame"]?.try(&.as_i64.to_i) || DEFAULT_MAX_FRAME
      framing.max_frame = max_frame
      features = [] of String
      w["body"]["features"]?.try &.as_a?.try &.each do |f|
        s = f.as_s?
        features << s if s
      end
      send0.call({
        "protocol"   => JSON::Any.new(PROTOCOL_ID),
        "version"    => JSON::Any.new(PROTOCOL_VERSION),
        "type"       => JSON::Any.new("component.register"),
        "message_id" => JSON::Any.new("reg1"),
        "session_id" => JSON::Any.new(session),
        "body"       => JSON::Any.new({
          "manifest" => JSON::Any.new({"id" => JSON::Any.new(logical)}),
        }),
      })
      reg = framing.read
      raise "register rejected: #{reg.inspect}" unless reg.try(&.["type"]?.try(&.as_s?)) == "registered"
      r = reg.not_nil!
      instance = r["instance_id"].as_s
      generation = r["generation"].as_s
      act = framing.read
      raise "expected activate, got #{act.try(&.["type"]?)}" unless act.try(&.["type"]?.try(&.as_s?)) == "lifecycle.activate"
      a = act.not_nil!
      bindings = [] of DepBinding
      a["body"]["dependency_bindings"]?.try &.as_a?.try &.each do |b|
        id = b["binding_id"]?.try &.as_s?
        cap = b["capability"]?.try &.as_s?
        bindings << DepBinding.new(id, cap) if id && cap && !id.empty? && !cap.empty?
      end
      op = a["body"]["operation_id"]? || JSON::Any.new("op?")
      comp = Component.new(sock, framing, session, instance, generation, features, bindings)
      comp.send_envelope("lifecycle.result", "lc1", a["request_id"]?.try(&.as_s?), {
        "operation_id" => op,
        "status"       => JSON::Any.new("ok"),
        "pending"      => JSON::Any.new([] of JSON::Any),
      })
      # The pump fiber already feeds framing; serve_loop consumes it.
      comp
    end

    # Serves until EOF/error, quiesce, or dispose. Returns exit reason.
    def serve(handler : Handler) : String
      stop = Channel(Nil).new(1)
      spawn dispatcher(handler, stop)
      reason = serve_loop(handler)
      stop.send(nil)
      reason
    ensure
      @sock.close rescue nil
    end

    private def dispatcher(handler : Handler, stop : Channel(Nil)) : Nil
      loop do
        select
        when stop.receive
          drain(handler)
          return
        when timeout(100.milliseconds)
          drain(handler)
        when @ev_wake.receive
          drain(handler)
        end
      end
    end

    private def drain(handler : Handler) : Nil
      batch = @ev_mu.synchronize do
        items = @ev_queue.to_a
        @ev_queue.clear
        items
      end
      batch.each do |it|
        begin
          if it.kind == :stream
            handler.on_stream(it.stream_id, it.seq, it.text)
          else
            handler.on_event(it.topic, it.payload)
          end
        rescue
          # handler bugs never kill the session
        end
      end
    end

    private def enqueue(it : EvItem) : Nil
      @ev_mu.synchronize do
        if @ev_queue.size >= EVENT_CAP
          @ev_queue.shift
          @ev_dropped.add(1)
        end
        @ev_queue << it
      end
      select
      when @ev_wake.send(nil) then nil
      else                        nil
      end
    end

    private def bound_ok?(env : JSON::Any) : Bool
      return false unless env["session_id"]?.try(&.as_s?) == @session_id
      if (inst = env["instance_id"]?.try(&.as_s?)) && inst != @instance_id
        return false
      end
      if (gen = env["generation"]?.try(&.as_s?)) && !Matrix.gen_equal?(gen, @generation)
        return false
      end
      true
    end

    private def reply_lifecycle(op : JSON::Any, request_id : String?) : Nil
      send_envelope("lifecycle.result", "m-lc-#{op.as_s? || "op"}", request_id, {
        "operation_id" => op,
        "status"       => JSON::Any.new("ok"),
        "pending"      => JSON::Any.new([] of JSON::Any),
      })
    end

    private def serve_loop(handler : Handler) : String
      framing = @framing
      loop do
        env = begin
          framing.read
        rescue
          return "eof"
        end
        return "eof" if env.nil?
        next if env["__malformed"]?.try(&.as_bool?) == true
        next unless bound_ok?(env)
        body = env["body"]? || JSON::Any.new({} of String => JSON::Any)
        type = env["type"]?.try(&.as_s?) || ""
        request_id = env["request_id"]?.try &.as_s?
        case type
        when "lifecycle.prepare", "lifecycle.activate", "lifecycle.quiesce"
          reply_lifecycle(body["operation_id"]? || JSON::Any.new("op?"), request_id)
        when "lifecycle.dispose"
          reply_lifecycle(body["operation_id"]? || JSON::Any.new("op?"), request_id)
          return "dispose"
        when "call.open"
          ticket = body["ticket"]?.try(&.as_s?) || ""
          cap = body["capability"]?.try(&.as_s?) || ""
          input = body["input"]? || JSON::Any.new({} of String => JSON::Any)
          open_rid = request_id
          cancel = Atomic(Bool).new(false)
          @calls_mu.synchronize { @calls[ticket] = cancel }
          ctx = CallCtx.new(self, ticket, cancel)
          spawn do
            out : JSON::Any? = nil
            business : BusinessError? = nil
            sdk_err : SdkError? = nil
            begin
              out = handler.on_call(ctx, ticket, cap, input, cancel)
            rescue be : BusinessError
              business = be
            rescue se : SdkError
              sdk_err = se
            rescue ex
              business = BusinessError.new("internal", "handler: #{ex.message}")
            ensure
              @calls_mu.synchronize { @calls.delete(ticket) }
            end
            next if cancel.get # late after cancel: stay silent
            rbody =
              if business
                b = business.not_nil!
                {
                  "ticket" => JSON::Any.new(ticket),
                  "status" => JSON::Any.new("error"),
                  "error"  => JSON::Any.new({
                    "code"    => JSON::Any.new(b.code),
                    "message" => JSON::Any.new(b.message || ""),
                  }),
                }
              elsif sdk_err
                s = sdk_err.not_nil!
                {
                  "ticket" => JSON::Any.new(ticket),
                  "status" => JSON::Any.new("error"),
                  "error"  => JSON::Any.new({
                    "code"    => JSON::Any.new(s.code),
                    "message" => JSON::Any.new(s.detail),
                  }),
                }
              else
                {
                  "ticket" => JSON::Any.new(ticket),
                  "status" => JSON::Any.new("ok"),
                  "output" => out || JSON::Any.new(nil),
                }
              end
            begin
              send_envelope("call.result", "m-call-#{ticket}", open_rid, rbody)
            rescue
              # transport gone
            end
          end
        when "call.cancel"
          ticket = body["ticket"]?.try(&.as_s?) || ""
          @calls_mu.synchronize { @calls[ticket]?.try &.set(true) }
          begin
            handler.on_cancel(ticket)
          rescue
          end
        when "dependency.result"
          if request_id
            ch = @dep_mu.synchronize { @dep_waiters.delete(request_id) }
            if ch
              if body["status"]?.try(&.as_s?) == "ok"
                ch.send(body["output"]? || JSON::Any.new(nil))
              else
                err = body["error"]? || JSON::Any.new({} of String => JSON::Any)
                code = err["code"]?.try(&.as_s?) || "internal"
                msg = err["message"]?.try(&.as_s?) || "remote error"
                ch.send(DepError.new(code, msg))
              end
            end
          end
        when "dependency.accepted", "dependency.cancel.result"
          # progress only; no answer needed
        when "resource.result"
          if request_id
            ch = @res_mu.synchronize { @res_waiters.delete(request_id) }
            if ch
              if body["status"]?.try(&.as_s?) == "ok"
                extra = {} of String => JSON::Any
                body.as_h?.try &.each do |k, v|
                  extra[k] = v unless k == "operation_id" || k == "status"
                end
                ch.send(extra)
              else
                code = body["code"]?.try(&.as_s?) || "internal"
                msg = body["message"]?.try(&.as_s?) || "remote error"
                ch.send(ResError.new(code, msg))
              end
            end
          end
        when "event.deliver"
          topic = body["topic"]?.try(&.as_s?) || ""
          unless topic.empty?
            enqueue(EvItem.new(:event, topic, body["payload"]? || JSON::Any.new(nil), "", 0_u64, ""))
          end
        when "stream.data"
          sid = body["stream_id"]?.try(&.as_s?) || ""
          seq = body["seq"]?.try(&.as_s?).try &.to_u64?
          payload = body["payload"]?.try(&.as_s?)
          if !sid.empty? && seq && payload
            enqueue(EvItem.new(:stream, "", JSON::Any.new(nil), sid, seq, payload))
          end
        end
      end
    end
  end
end

require "./operator"

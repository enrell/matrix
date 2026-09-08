# Operator/application surface for Crystal (ML1, stdlib only).
#
# Same contract as the other SDKs: calls travel through the staged
# `matrix-managed` binary (`serve` to own a kernel, `request` for
# authenticated admin actions over mutual TLS). `Operator.start` owns
# its process; `Operator.connect` only attaches. Server identity never
# implies caller authority: `OperatorPki` is always explicit.
require "json"

module Matrix
  # `start`/`connect` failure. Nothing owned is left behind.
  class BootstrapError < SdkError
    def initialize(code : String, phase : String, message : String)
      super(code, phase, message)
    end
  end

  # Admin refusal/failure. Wire codes pass through; timeouts report
  # outcome-unknown and never retry implicitly.
  class OperatorError < SdkError
    def initialize(code : String, message : String)
      super(code, "request", message)
    end
  end

  # Runs a binary with an absolute deadline; kills past it. Returns
  # {status, stdout, stderr}. Spawn failures raise OperatorError.
  def self.run_capture(binary : String, args : Array(String),
                               timeout : Time::Span) : {Process::Status, String, String}
    proc = begin
      Process.new(binary, args, output: :pipe, error: :pipe)
    rescue ex
      raise OperatorError.new("transport", "spawn: #{ex.message}")
    end
    deadline = Time.instant + timeout
    # Drain pipes concurrently: `wait` closes them, so reads must be
    # in flight before reaping (small admin outputs, memory buffered).
    so_ch = Channel(String).new(1)
    se_ch = Channel(String).new(1)
    spawn do
      begin
        so_ch.send(proc.output.gets_to_end)
      rescue
        so_ch.send("")
      end
    end
    spawn do
      begin
        se_ch.send(proc.error.gets_to_end)
      rescue
        se_ch.send("")
      end
    end
    status : Process::Status? = nil
    loop do
      if proc.terminated?
        status = proc.wait
        break
      end
      if Time.instant >= deadline
        proc.terminate(graceful: false) rescue nil
        status = (proc.wait rescue nil)
        so_ch.receive
        se_ch.receive
        raise OperatorError.new("outcome-unknown",
          "admin action timed out after #{timeout.total_seconds}s (not retried)")
      end
      sleep 20.milliseconds
    end
    {status.not_nil!, so_ch.receive, se_ch.receive}
  end

  KNOWN_CODES = %w[
    permission-denied stale-generation outcome-unknown
    unauthenticated invalid-message unsupported-version
    dependency-unavailable ambiguous-provider
    context-not-active resource-exhausted deadline-exceeded
    cancelled cleanup-pending internal
  ]

  def self.guess_code(stderr : String) : String
    low = stderr.downcase
    KNOWN_CODES.each { |c| return c if low.includes?(c) }
    stderr.strip.empty? ? "internal" : "transport"
  end

  def self.redact(node : JSON::Any) : JSON::Any
    case raw = node.raw
    when Hash(String, JSON::Any)
      clean = {} of String => JSON::Any
      raw.each do |k, v|
        clean[k] = (k == "lease" || k == "launch_token" || k == "token") ? JSON::Any.new("<redacted>") : redact(v)
      end
      JSON::Any.new(clean)
    when Array(JSON::Any)
      JSON::Any.new(raw.map { |v| redact(v) })
    else
      node
    end
  end

  # Caller's mTLS identity (never the server's).
  struct OperatorPki
    getter ca : String
    getter cert : String
    getter key : String

    def initialize(@ca : String, @cert : String, @key : String)
    end
  end

  # Attached operator client. `close` never stops any daemon.
  class Client
    EXPECTED_API_PREFIX = "0.1."

    def initialize(@binary : String, @listen : String, @pki : OperatorPki,
                   @server_name : String, @owned : Bool)
      @closed = false
    end

    def listen : String
      @listen
    end

    private def check_open : Nil
      raise SdkError.new("internal", "client", "client is closed") if @closed
    end

    # One authenticated admin action; refusals keep wire codes.
    def request(action : Hash(String, JSON::Any),
                timeout : Time::Span = 30.seconds) : JSON::Any
      check_open
      raise OperatorError.new("invalid-message", "action hash with 'action' required") unless action.has_key?("action")
      args = ["request", @pki.ca, @pki.cert, @pki.key, @listen, @server_name, action.to_json]
      status, so, se = Matrix.run_capture(@binary, args, timeout + 10.seconds)
      err = (se + so).strip
      unless status.success?
        first = err.split('\n').first? || "request refused"
        raise OperatorError.new(Matrix.guess_code(err),
          first.empty? ? "request refused" : first)
      end
      begin
        JSON.parse(so)
      rescue ex
        raise OperatorError.new("internal", "undecodable response: #{ex.message}")
      end
    end

    def activate(component : String, ttl_ms : Int = 20000,
                 timeout : Time::Span = 30.seconds) : JSON::Any
      request({
        "action"    => JSON::Any.new("activate"),
        "component" => JSON::Any.new(component),
        "ttl_ms"    => JSON::Any.new(ttl_ms.to_i64),
      }, timeout)
    end

    def status(lease : String, fence : String,
               timeout : Time::Span = 30.seconds) : JSON::Any
      request({
        "action" => JSON::Any.new("status"),
        "lease"  => JSON::Any.new(lease),
        "fence"  => JSON::Any.new(fence),
      }, timeout)
    end

    def invoke(lease : String, fence : String, operation : String, cap : String,
               input : JSON::Any, timeout : Time::Span = 30.seconds) : JSON::Any
      request({
        "action"    => JSON::Any.new("invoke"),
        "lease"     => JSON::Any.new(lease),
        "fence"     => JSON::Any.new(fence),
        "operation" => JSON::Any.new(operation),
        "cap"       => JSON::Any.new(cap),
        "input"     => input,
      }, timeout)
    end

    def release(lease : String, fence : String,
                timeout : Time::Span = 30.seconds) : JSON::Any
      request({
        "action" => JSON::Any.new("release"),
        "lease"  => JSON::Any.new(lease),
        "fence"  => JSON::Any.new(fence),
      }, timeout)
    end

    def renew(lease : String, fence : String, ttl_ms : Int = 20000,
              timeout : Time::Span = 30.seconds) : JSON::Any
      request({
        "action" => JSON::Any.new("renew"),
        "lease"  => JSON::Any.new(lease),
        "fence"  => JSON::Any.new(fence),
        "ttl_ms" => JSON::Any.new(ttl_ms.to_i64),
      }, timeout)
    end

    # Polls status until the session reports ready.
    def wait_ready(lease : String, fence : String,
                   timeout : Time::Span = 20.seconds) : JSON::Any
      deadline = Time.instant + timeout
      last = JSON::Any.new(nil)
      while Time.instant < deadline
        last = status(lease, fence, 5.seconds)
        return last if last["ready"]?.try(&.as_bool?) == true
        sleep 100.milliseconds
      end
      raise OperatorError.new("outcome-unknown",
        "session not ready in budget (last=#{Matrix.redact(last).to_json})")
    end

    # Marks this handle closed. Never stops any daemon.
    def close : Nil
      @closed = true
    end
  end

  # A kernel this application started and owns.
  class OwnedKernel
    getter client : Client
    getter epoch : JSON::Any
    getter api : String
    getter profile : String

    def initialize(@proc : Process, @workdir : String, @client : Client,
                   @epoch : JSON::Any, @api : String, @profile : String)
      @closed = false
    end

    def listen : String
      @client.listen
    end

    # Idempotent: SIGTERM, bounded wait, SIGKILL, remove the private
    # directory. Reaps exactly the spawned daemon.
    def close : Nil
      return if @closed
      @closed = true
      @client.close
      begin
        unless @proc.terminated?
          @proc.terminate(graceful: true) rescue nil
          50.times do
            break if @proc.terminated?
            sleep 100.milliseconds
          end
          unless @proc.terminated?
            @proc.terminate(graceful: false) rescue nil
            @proc.wait rescue nil
          end
        end
      ensure
        FileUtils.rm_rf(@workdir) rescue nil
      end
    end
  end

  module Operator
    READY_TIMEOUT = 30.seconds

    # Attaches to an existing kernel. The client owns no process.
    def self.connect(binary : String, listen : String, pki : OperatorPki,
                     server_name : String = "localhost") : Client
      {"binary" => binary, "ca" => pki.ca, "cert" => pki.cert, "key" => pki.key}.each do |label, path|
        raise BootstrapError.new("transport", "connect", "#{label} not found: #{path}") unless File.exists?(path)
      end
      raise BootstrapError.new("invalid-message", "connect", "listen address required") if listen.empty?
      Client.new(binary, listen, pki, server_name, false)
    end

    # Starts an owned kernel from a config hash. `operator_pki` is the
    # caller's identity and is required; failures reap everything created.
    def self.start(binary : String, config : Hash(String, JSON::Any),
                   operator_pki : OperatorPki,
                   server_name : String = "localhost") : OwnedKernel
      unless File::Info.executable?(binary)
        raise BootstrapError.new("transport", "spawn", "binary not executable: #{binary}")
      end
      home = config["home"]?.try &.as_s?
      if home.nil? || home.empty?
        raise BootstrapError.new("invalid-message", "config", "config hash with 'home' required")
      end
      if operator_pki.ca.empty? || operator_pki.cert.empty? || operator_pki.key.empty?
        raise BootstrapError.new("invalid-message", "config",
          "operator PKI (ca/cert/key) is required: server identity never implies caller authority")
      end
      {"ca" => operator_pki.ca, "cert" => operator_pki.cert, "key" => operator_pki.key}.each do |label, path|
        unless File.exists?(path)
          raise BootstrapError.new("invalid-message", "config", "operator #{label} not found: #{path}")
        end
      end
      workdir = File.join(Dir.tempdir, "mx-cr-#{Random::Secure.hex(8)}")
      Dir.mkdir_p(workdir)
      proc : Process? = nil
      begin
        cfg_path = File.join(workdir, "config.json")
        File.write(cfg_path, config.to_json)
        r_out, w_out = IO.pipe
        r_err, w_err = IO.pipe
        proc = Process.new(binary, ["serve", cfg_path], output: w_out, error: w_err)
        w_out.close
        w_err.close
        ready = read_ready(proc, r_out, r_err)
        listen = ready["listen"]?.try(&.as_s?) || ""
        if listen.empty?
          tls = config["tls"]?
          listen = tls.try(&.["listen"]?.try(&.as_s?)) || ""
        end
        api = ready["api"]?.try(&.as_s?) || ""
        if !api.empty? && !api.starts_with?(Client::EXPECTED_API_PREFIX)
          raise BootstrapError.new("unsupported-version", "version",
            "binary api outside #{Client::EXPECTED_API_PREFIX}x: #{api}")
        end
        client = Client.new(binary, listen, operator_pki, server_name, true)
        OwnedKernel.new(proc, workdir, client,
          ready["epoch"]? || JSON::Any.new(nil), api,
          ready["profile"]?.try(&.as_s?) || "")
      rescue ex : BootstrapError
        reap(proc, workdir)
        raise ex
      rescue ex
        reap(proc, workdir)
        raise BootstrapError.new("transport", "spawn", ex.message || "start failed")
      end
    end

    private def self.reap(proc : Process?, workdir : String) : Nil
      if proc && !proc.terminated?
        proc.terminate(graceful: false) rescue nil
        proc.wait rescue nil
      end
      FileUtils.rm_rf(workdir) rescue nil
    end

    private def self.read_ready(proc : Process, stdout : IO, stderr : IO) : Hash(String, JSON::Any)
      line_ch = Channel(String?).new(1)
      spawn do
        begin
          line_ch.send(stdout.gets)
        rescue
          line_ch.send(nil)
        end
      end
      line : String? = nil
      deadline = Time.instant + READY_TIMEOUT
      loop do
        select
        when l = line_ch.receive
          line = l
          break
        when timeout(200.milliseconds)
          if proc.terminated?
            first = stderr.gets_to_end.strip.split('\n').first? || "daemon exited"
            raise BootstrapError.new("internal", "config", "daemon refused config: #{first}")
          end
          if Time.instant >= deadline
            raise BootstrapError.new("transport", "ready", "no ready line in budget")
          end
        end
      end
      raise BootstrapError.new("transport", "ready", "no ready line in budget") if line.nil?
      ready = begin
        JSON.parse(line).as_h
      rescue
        raise BootstrapError.new("transport", "ready", "undecodable ready line")
      end
      raise BootstrapError.new("transport", "ready", "daemon not ready") unless ready["ready"]?.try(&.as_bool?) == true
      ready
    end

    # Environment diagnosis (no secrets).
    def self.doctor(binary : String?) : Hash(String, JSON::Any)
      errors = [] of JSON::Any
      rep = {
        "crystal"              => JSON::Any.new(Crystal::VERSION),
        "binary"               => JSON::Any.new(binary || ""),
        "binary_found"         => JSON::Any.new(false),
        "binary_executable"    => JSON::Any.new(false),
        "cli_shape_ok"         => JSON::Any.new(false),
        "openssl"              => JSON::Any.new(!Process.find_executable("openssl").nil?),
        "bwrap"                => JSON::Any.new(!Process.find_executable("bwrap").nil?),
        "socket_dir_writable"  => JSON::Any.new(false),
        "errors"               => JSON::Any.new(errors),
      }
      if b = binary
        if File.exists?(b)
          rep["binary_found"] = JSON::Any.new(true)
          if File::Info.executable?(b)
            rep["binary_executable"] = JSON::Any.new(true)
            begin
              _, so, se = Matrix.run_capture(b, [] of String, 10.seconds)
            rescue
              so, se = "", ""
            end
            if (so + se).includes?("matrix-managed serve")
              rep["cli_shape_ok"] = JSON::Any.new(true)
            else
              errors << JSON::Any.new("binary does not speak the managed CLI shape")
            end
          else
            errors << JSON::Any.new("binary not executable")
          end
        else
          errors << JSON::Any.new("binary not found: set it explicitly or via PATH (no silent download)")
        end
      else
        errors << JSON::Any.new("binary not found: set it explicitly or via PATH (no silent download)")
      end
      begin
        dir = File.join(Dir.tempdir, "mx-doc-#{Random::Secure.hex(4)}")
        Dir.mkdir(dir)
        begin
          UNIXServer.new(File.join(dir, "t.sock")).close
          rep["socket_dir_writable"] = JSON::Any.new(true)
        rescue ex
          errors << JSON::Any.new("unix socket probe failed: #{ex.message}")
        ensure
          FileUtils.rm_rf(dir) rescue nil
        end
      rescue ex
        errors << JSON::Any.new("temp dir probe failed: #{ex.message}")
      end
      rep
    end
  end
end

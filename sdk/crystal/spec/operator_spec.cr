require "spec"
require "../src/matrix-component"

# Operator-surface specs: mapping over a fake binary, bootstrap
# phases, doctor shape; live parts need MX_MATRIX_MANAGED + MX_DEV_PKI.
module OpHelper
  def self.write_fake_binary : String
    dir = File.join(Dir.tempdir, "opfake-#{Random::Secure.hex(4)}")
    Dir.mkdir_p(dir)
    fake = File.join(dir, "matrix-managed")
    File.write(fake, <<-SH)
      #!/bin/sh
      if [ "$1" = "request" ]; then
        case "$7" in
          *sleep*) exec sleep 30;;
          *badjson*) echo 'not json';;
          *denied*) echo 'permission-denied: nope' >&2; exit 1;;
          *) echo '{"ok":true}';;
        esac
      else echo 'usage: matrix-managed serve <config>' >&2; exit 1
      fi
      SH
    File.chmod(fake, 0o755)
    %w[ca cert key].each { |n| File.write(File.join(dir, n), "") }
    dir
  end

  def self.live_binary : String?
    env = ENV["MX_MATRIX_MANAGED"]?
    return env if env && File.exists?(env)
    nil
  end

  def self.live_pki_script : String?
    env = ENV["MX_DEV_PKI"]?
    return env if env && File.exists?(env)
    nil
  end
end

describe "operator mapping" do
  it "maps CLI outcomes to typed errors" do
    dir = OpHelper.write_fake_binary
    begin
      fake = File.join(dir, "matrix-managed")
      pki = Matrix::OperatorPki.new(File.join(dir, "ca"), File.join(dir, "cert"), File.join(dir, "key"))
      c = Matrix::Operator.connect(fake, "127.0.0.1:9", pki)
      pong = c.request({"action" => JSON::Any.new("ping")})
      pong["ok"].as_bool.should be_true
      expect_raises(Matrix::OperatorError, /permission-denied/) do
        c.request({"action" => JSON::Any.new("denied-op")})
      end
      begin
        c.request({"action" => JSON::Any.new("badjson")})
        raise "badjson succeeded"
      rescue ex : Matrix::OperatorError
        ex.code.should eq "internal"
      end
      begin
        c.request({"action" => JSON::Any.new("sleep")}, 1.second)
        raise "sleep succeeded"
      rescue ex : Matrix::OperatorError
        ex.code.should eq "outcome-unknown"
      end
      begin
        c.request({} of String => JSON::Any)
        raise "actionless succeeded"
      rescue ex : Matrix::OperatorError
        ex.code.should eq "invalid-message"
      end
      c.close
      expect_raises(Matrix::SdkError) do
        c.request({"action" => JSON::Any.new("ping")})
      end
    ensure
      FileUtils.rm_rf(dir) rescue nil
    end
  end

  it "bootstrap phases are explicit" do
    expect_raises(Matrix::BootstrapError, /spawn|binary/) do
      Matrix::Operator.start("/nonexistent/matrix-managed",
        {"home" => JSON::Any.new("/tmp/x")}, Matrix::OperatorPki.new("", "", ""))
    end
    begin
      Matrix::Operator.start(Process.executable_path || "/bin/true",
        {} of String => JSON::Any, Matrix::OperatorPki.new("", "", ""))
      raise "homeless config started"
    rescue ex : Matrix::BootstrapError
      ex.phase.should eq "config"
    end
    expect_raises(Matrix::BootstrapError) do
      Matrix::Operator.connect("/nonexistent/x", "127.0.0.1:1",
        Matrix::OperatorPki.new("a", "b", "c"))
    end
  end

  it "doctor shape carries no secrets" do
    rep = Matrix::Operator.doctor("/nonexistent/binary")
    %w[crystal binary binary_found cli_shape_ok openssl bwrap socket_dir_writable errors].each do |k|
      rep.has_key?(k).should be_true
    end
    rep.to_json.gsub("socket_dir_writable", "").includes?("lease").should be_false
    if bin = OpHelper.live_binary
      rep2 = Matrix::Operator.doctor(bin)
      rep2["binary_found"].as_bool.should be_true
      rep2["cli_shape_ok"].as_bool.should be_true
    end
  end
end

describe "operator live" do
  it "start/attach lifecycle with stable denial codes" do
    bin = OpHelper.live_binary
    pki_script = OpHelper.live_pki_script
    pending!("needs MX_MATRIX_MANAGED + MX_DEV_PKI") if bin.nil? || pki_script.nil?
    tmp = File.join(Dir.tempdir, "oplive-#{Random::Secure.hex(4)}")
    Dir.mkdir_p(tmp)
    begin
      pki = File.join(tmp, "pki")
      pk_out = IO::Memory.new
      status = Process.run("python3",
        [pki_script.not_nil!, pki, "--server-name", "localhost"],
        output: pk_out, error: :inherit)
      raise "dev-pki failed" unless status.success?
      m = pk_out.to_s.match(/client fingerprint: ([0-9a-f]{64})/)
      raise "no fingerprint in dev-pki output" if m.nil?
      fingerprint = m[1]
      cfg = {
        "home"       => JSON::Any.new(File.join(tmp, "home")),
        "components" => JSON::Any.new([JSON::Any.new({
          "manifest" => JSON::Any.new({
            "id"           => JSON::Any.new("echo"),
            "capabilities" => JSON::Any.new([JSON::Any.new("echo.msg@1")]),
            "reducer"      => JSON::Any.new("echo"),
          }),
          "trusted" => JSON::Any.new(true),
        })]),
        "grants" => JSON::Any.new({
          fingerprint => JSON::Any.new({
            "components"   => JSON::Any.new([JSON::Any.new("echo")]),
            "capabilities" => JSON::Any.new([JSON::Any.new("echo.msg@1")]),
          }),
        }),
        "tls" => JSON::Any.new({
          "listen" => JSON::Any.new("127.0.0.1:0"),
          "ca"     => JSON::Any.new(File.join(pki, "ca.der")),
          "cert"   => JSON::Any.new(File.join(pki, "server.der")),
          "key"    => JSON::Any.new(File.join(pki, "server-key.der")),
        }),
      }
      opki = Matrix::OperatorPki.new(File.join(pki, "ca.der"),
        File.join(pki, "client.der"), File.join(pki, "client-key.der"))
      kernel = Matrix::Operator.start(bin.not_nil!, cfg, opki)
      begin
        kernel.api.starts_with?("0.1.").should be_true
        act = kernel.client.activate("echo", 20000)
        lease = act["lease"].as_s
        fence = act["fence"].as_s
        v = kernel.client.invoke(lease, fence, "op-live-1", "echo.msg@1",
          JSON::Any.new({"ping" => JSON::Any.new(1_i64)}))
        v["ok"].as_bool.should be_true
        attached = Matrix::Operator.connect(bin.not_nil!, kernel.listen, opki)
        v2 = attached.invoke(lease, fence, "op-live-2", "echo.msg@1",
          JSON::Any.new({} of String => JSON::Any))
        v2["ok"].as_bool.should be_true
        attached.close # attachment owns nothing: daemon keeps serving
        v3 = kernel.client.invoke(lease, fence, "op-live-3", "echo.msg@1",
          JSON::Any.new({} of String => JSON::Any))
        v3["ok"].as_bool.should be_true
        expect_raises(Matrix::OperatorError) do
          kernel.client.invoke("dead", "1", "op-x", "echo.msg@1",
            JSON::Any.new({} of String => JSON::Any))
        end
        kernel.client.release(lease, fence)
      ensure
        kernel.close
      end
      expect_raises(Exception) do
        kernel.client.invoke("x", "1", "op-x", "echo.msg@1",
          JSON::Any.new({} of String => JSON::Any))
      end
    ensure
      FileUtils.rm_rf(tmp) rescue nil
    end
  end
end

# matrix-component (Crystal SDK)

Experimental Matrix SDK for Crystal (ML1 contract surface, version
`0.1.0`). Dual-licensed MIT OR Apache-2.0 (see LICENSE-MIT,
LICENSE-APACHE-2.0); registry publication in progress.

Contract reference: `docs/SDK.md`, `docs/ML1-NODE.md`,
`docs/ML1-MATRIX.md` in the Matrix repository. Same observable
behavior as the reference SDKs (Rust `matrix-component`, Python
`matrix_component`).

## Install

No dependencies beyond the Crystal standard library. Crystal >= 1.0
(tested 1.21.0, linux/x86_64):

```sh
# offline: copy the SDK dir (or scaffold, which vendors it)
cp -r /path/to/staged/sdk/crystal ./vendor/matrix-component
```

`shards build` needs no network (zero dependencies).

## Component side

```crystal
require "./src/matrix-component"

class Echo < Matrix::Handler
  def on_call(ctx : Matrix::CallCtx, ticket : String, cap : String,
              input : JSON::Any, cancel : Atomic(Bool)) : JSON::Any
    if input["chain"]?.try(&.as_bool?) == true
      dep_out = ctx.invoke_dependency(ctx.dependencies[0].id,
        input["input"]? || JSON::Any.new({} of String => JSON::Any), 5.seconds)
      return JSON::Any.new({"chained" => dep_out})
    end
    JSON::Any.new({"echo" => input})
  end

  def on_event(topic : String, payload : JSON::Any) : Nil
    # observe fast, never block the dispatcher
  end
end

comp = Matrix::Component.connect(ENV["MATRIX_SOCK"], "echo")
comp.serve(Echo.new) # "dispose" | "eof"
```

- One fiber per call with a cooperative `Atomic(Bool)` cancel flag;
  `on_cancel(ticket)` observes cancellation. Late answers after
  cancel stay silent. Handlers raise `Matrix::BusinessError` for
  business errors (code preserved on the wire).
- `send_stream(stream_id, seq, text)`: text only — the `Bytes`
  overload raises `invalid-message`, never lossy-converts.
- `u64` wire values are decimal strings compared with
  `Matrix.gen_equal?` (full precision; out-of-range is stale/invalid,
  never wrapped).
- Events/streams share a bounded edge queue (64, drop-oldest,
  counted in `event_dropped_count`); the dispatcher fiber never runs
  on the read loop, so slow observers throttle via host credit
  instead of stalling calls.
- Without a negotiated `dependency-calls/1`, `invoke_dependency`
  raises `DepError("unsupported-feature")` without touching the wire.
- Cooperative scheduling note: blocking calls poll in 50 ms slices
  so cancel/deadline preempt the wait without stalling the reader.

## Operator side

```crystal
kernel = Matrix::Operator.start("/path/to/matrix-managed", config, opki)
act = kernel.client.activate("prov", 20000)
v = kernel.client.invoke(act["lease"].as_s, act["fence"].as_s, "op-1",
  "prov.echo@1", JSON::Any.new({"ping" => JSON::Any.new(1_i64)}))
kernel.close # owned: reaps only this daemon
```

Operator calls travel through the staged `matrix-managed` binary
(`serve`/`request`, mutual TLS). `start` owns its daemon; `connect`
attaches (closing never stops a shared kernel). `OperatorPki` is
always explicit. Timeouts report `outcome-unknown`, never retry.

## Scaffold, doctor, tests

```sh
./scaffold.sh myapp ./myapp --pki ./pki --home ./priv-home  # vendors SDK, builds node
crystal spec                         # loopback suites; live parts need MX_MATRIX_MANAGED + MX_DEV_PKI
crystal run examples/doctor.cr -- --binary /path/to/matrix-managed
```

`matrix-doctor` prints environment diagnosis as JSON and redacts
secrets. Scaffolded projects are covered by `templates/README.md`.

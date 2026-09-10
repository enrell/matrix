# matrix-component-go (Go SDK)

Experimental Matrix SDK for Go (ML1 contract surface, version
`0.1.0`). Module `matrix-component-go`, package `component`.
Dual-licensed MIT OR Apache-2.0 (see LICENSE-MIT, LICENSE-APACHE-2.0);
registry publication in progress.

Contract reference: `docs/SDK.md`, `docs/ML1-NODE.md`,
`docs/ML1-MATRIX.md` in the Matrix repository. Same observable
behavior as the reference SDKs.

## Install

No dependencies beyond the standard library. Go >= 1.24 (tested on
1.27.1, linux/amd64):

```sh
# inside your module (offline: copy the SDK dir, add a replace line)
require matrix-component-go v0.1.0
replace matrix-component-go => /path/to/staged/sdk/go
```

`GOPROXY=off` builds work: nothing is fetched.

## Component side

```go
import mx "matrix-component-go"

type Echo struct{ mx.BaseHandler }

func (Echo) OnCall(ctx *mx.CallCtx, ticket, cap string, input any, callCtx context.Context) (any, *mx.SdkError) {
    in, _ := input.(map[string]any)
    if on, _ := in["chain"].(bool); on {
        out, derr := ctx.InvokeDependency(ctx.Dependencies()[0].ID, in["input"], 5*time.Second)
        if derr != nil {
            return nil, derr
        }
        return map[string]any{"chained": out}, nil
    }
    return map[string]any{"echo": in}, nil
}

comp, err := mx.Connect(sockPath, "echo") // MATRIX_LAUNCH_TOKEN via env
reason := comp.Serve(Echo{})              // "dispose" | "eof"
```

- One goroutine per call bound to a cancellable `context.Context`;
  cancel arrives as context cancellation plus `OnCancel(ticket)`.
  Late answers after cancel stay silent.
- `SendStream(streamId, seq, text)`: text only. `SendStreamBytes`
  refuses binary explicitly (`invalid-message`), never lossy-converts.
- `u64` wire values are decimal strings compared with `GenEqual`
  (no precision loss; out-of-range is stale/invalid, never wrapped).
- Events/streams share a bounded edge queue (64, drop-oldest,
  counted in `EventDroppedCount()`); the dispatcher goroutine never
  runs on the read loop, so slow observers throttle via host credit
  instead of stalling calls. Handler panics are contained per item.
- Without a negotiated `dependency-calls/1`, `InvokeDependency`
  returns `unsupported-feature` without touching the wire.
- Reader-guard note: handlers always run off the read loop, so Go
  needs no thread-identity guard (documented difference from
  Rust/Python; same guarantee).

## Operator side

```go
kernel, err := mx.Start("/path/to/matrix-managed", config, mx.OperatorPKI{CA: ca, Cert: cert, Key: key}, "")
act, _ := kernel.ClientOf().Activate("prov", 20000, 0)
v, _ := kernel.ClientOf().Invoke(act["lease"].(string), act["fence"].(string),
    "op-1", "prov.echo@1", map[string]any{"ping": 1}, 0)
kernel.Close() // owned: reaps only this daemon
```

Operator calls travel through the staged `matrix-managed` binary
(`serve`/`request`, mutual TLS). `Start` owns its daemon; `Attach`
only connects (closing never stops a shared kernel). `OperatorPKI`
is always explicit. Timeouts report `outcome-unknown`, never retry.

## Scaffold, doctor, tests

```sh
./scaffold.sh myapp ./myapp --pki ./pki --home ./priv-home  # vendors SDK, builds node
go build -o /tmp/mx-doctor ./cmd/matrix-doctor && /tmp/mx-doctor --binary /path/to/matrix-managed
GOPROXY=off go vet ./... && GOPROXY=off go test -count=1 .
```

`matrix-doctor` prints environment diagnosis as JSON and redacts
secrets. Scaffolded projects are covered by `templates/README.md`.

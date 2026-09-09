# matrix-component (Elixir SDK)

Experimental Matrix SDK for Elixir (ML1 contract surface, version
`0.1.0`). Local validation artifact only — not for Hex publication
(the repository ships no license file).

Contract reference: `docs/SDK.md`, `docs/ML1-NODE.md`,
`docs/ML1-MATRIX.md` in the Matrix repository. Same observable
behavior as the reference SDKs (Rust `matrix-component`, Python
`matrix_component`).

## Install

No dependencies beyond Elixir/OTP stdlib (`:json` needs OTP >= 27).
Elixir >= 1.17 (tested 1.20 with OTP 29, linux/x86_64):

```elixir
# offline: copy the SDK dir (or scaffold, which stages it)
{:matrix_component, path: "/path/to/staged/sdk/elixir"}
```

`mix deps.get` fetches nothing; `mix test` and `mix escript.build`
work offline.

## Component side

```elixir
defmodule Echo do
  use Matrix.Handler

  @impl true
  def on_call(ctx, _ticket, _cap, input, _ref) do
    if input["chain"] == true do
      [first | _] = Matrix.CallCtx.dependencies(ctx)
      out = Matrix.CallCtx.invoke_dependency(ctx, first.id, input["input"] || %{}, 5_000)
      {:ok, %{"chained" => out}}
    else
      {:ok, %{"echo" => input}}
    end
  end
end

{:ok, reader} = Matrix.Component.connect(System.get_env("MATRIX_SOCK"), "echo")
Matrix.Component.serve(reader, Echo) # :dispose | :eof
```

- One supervised task per call; cancel arrives as
  `{:matrix_cancel, ticket}` in the task mailbox
  (`CallCtx.cancelled?/1` polls without consuming other messages)
  plus `on_cancel/1`. Late answers after cancel stay silent.
  Handlers return `{:ok, output}` or `{:error, code, message}`, or
  raise `Matrix.Errors.BusinessError`.
- `send_stream(ctx, id, seq, text)`: text only — non-UTF-8 binaries
  raise `invalid-message`, never lossy-converted.
- `u64` wire values are decimal strings compared with
  `Matrix.Framing.gen_equal?` (full precision; out-of-range is
  stale/invalid, never wrapped).
- Events/streams share a bounded edge queue in the reader (64,
  drop-oldest, counted in `event_dropped_count/1`); the dispatcher
  process pulls batches, so slow observers throttle via host credit
  instead of stalling calls.
- Without a negotiated `dependency-calls/1`, `invoke_dependency`
  raises `DepError` (`unsupported-feature`) without touching the wire.
- Supervision: the reader is linked to the connecting process — its
  death takes the session down (no orphan context), and the SDK never
  resurrects an old generation (documented difference from
  auto-restart supervisors, same guarantee).

## Operator side

```elixir
kernel = Matrix.Operator.start("/path/to/matrix-managed", config, opki)
act = Matrix.Operator.activate(kernel.client, "prov", 20_000)
v = Matrix.Operator.invoke(kernel.client, act["lease"], act["fence"],
  "op-1", "prov.echo@1", %{"ping" => 1})
Matrix.Operator.close(kernel) # owned: reaps only this daemon
```

Operator calls travel through the staged `matrix-managed` binary
(`serve`/`request`, mutual TLS). `start` owns its daemon (SIGTERM,
bounded wait, SIGKILL — `Port.close/1` alone never signals);
`connect` attaches (closing never stops a shared kernel). The
operator PKI map is always explicit. Timeouts report
`outcome-unknown`, never retry.

## Scaffold, doctor, tests

```sh
./scaffold.sh myapp ./myapp --pki ./pki --home ./priv-home  # stages SDK, builds escript node
mix test                  # loopback suites; live parts need MX_MATRIX_MANAGED + MX_DEV_PKI
mix run -e 'Matrix.Doctor.main()' -- --binary /path/to/matrix-managed
```

`Matrix.Doctor` prints environment diagnosis as JSON and redacts
secrets. Scaffolded projects are covered by `templates/README.md`.

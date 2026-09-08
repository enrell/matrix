# Version policy (M8, experimental contract)

All surfaces below are `0.x` **experimental**: breaking changes are
allowed, but must be detectable before partial operation, with explicit
refusal and a migration note. No `1.0` is declared by this epic.

| Surface | Current | Numbering |
|---|---|---|
| Rust crates (`matrix-*`) | `0.1.0`, `publish = false` | Independent per crate; relation published here when they diverge |
| Facade contract (`api::API_VERSION`) | `0.1.0-experimental` | Bumps on any supported-surface break |
| Python `matrix-component` | `0.1.0` | Follows the component protocol, not crate numbers |
| ML1 SDKs (JS/TS, Go, Crystal, Elixir, C#, C, C++) | `0.1.0` each | Follow the component protocol + ML1 node contract, not crate numbers |
| Component protocol (`matrix.component`) | `0.1` | Negotiated `features`; unknown types fail closed |
| Remote profile (`matrix.remote`) | `0.1` | ALPN + `features`; missing capability refuses explicitly |
| Managed profile (`matrix.managed`) | `0.1` | Unary admin actions; additive only |
| Inspect snapshot (`matrix.inspect`) | `1` | Additive fields only; readers ignore unknowns |
| Manifest/config schema | Unversioned JSON, `deny_unknown_fields` | Unknown fields reject (detectable before operating) |
| Store format (`user_version`) | `1` | Open refuses other versions; backups record it |

No license file ships with this repository, so registry publication is
disabled (`publish = false`, no PyPI upload): distribution is local
artifacts with recorded hashes only (see `scripts/package.sh` and the
M8 delivery record). Adding a license is a project decision, not an
implementer patch.

Kernel updates (new activation semantics, ledger rules) and component
updates (new manifest version → new generation, old handles invalid)
are different contracts: the first needs operator restart + reconcile,
the second is routine activation turnover. Incompatible legacy removal
ships migration notes and clear refusal — never silent aliases that
reintroduce application semantics into the kernel.

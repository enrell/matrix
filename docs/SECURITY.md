# Security and authority boundaries

> Available implementation: [managed profile M3–M5](MANAGED-RUNTIME.md). This document preserves the design contract; see the profile for which mechanisms and transports are implemented.

Status: proposed requirements; the current scaffold does not provide this security model.

## Threat model

Assume buggy or malicious plugins, compromised remote peer, malformed payloads, resource exhaustion, and stale responses after replacement. The kernel and the hosts enforcing isolation belong to the trusted base. Administrative compromise of the host escapes local guarantees.

Rust reduces classes of memory errors in safe code; it does not prevent logic failures, deadlocks, permission abuse, incorrect unsafe, or actions by an external process. In-process plugins are explicitly trusted.

## Authority

Authentication identifies the peer. Authorization limits each action by principal, context, resource, interface, and operation. The manifest requests permissions; trusted configuration grants or denies. A capability name is not a credential. Do not execute commands or widen permissions because a plugin requested it in text.

Grants are issued by the kernel, bound to session/instance/generation, scoped, and revocable. Transitive calls do not implicitly widen authority; services validate the effective caller to avoid using their own permissions on behalf of a plugin without access.

## Profiles

| Profile | Required protection | Limit |
|---|---|---|
| Trusted in-process | Contexts and accounting | Plugin can violate the process; no strong containment |
| Isolated process | User/permissions, limits, process group, restricted mounts and network | Spawn alone is not a sandbox |
| WASM | Granted imports, bounded memory, and interruption | Host capabilities still need authorization |
| Remote | Authenticated channel, scoped grants, trusted host, and fencing | Kernel cannot directly clean up a partitioned remote OS |

Concrete Linux isolation tooling will be chosen in M3. The profile may only be advertised when it demonstrates containment of descendants, file access, network, CPU, and memory.

## Workspace and secrets

The executor receives an identified snapshot/revision and only the data it needs. Writes stay in a private workspace. Applying to the user's workspace is an explicit operation with base-revision validation and conflict detection; discard is separate.

Secrets are granted only to the component that needs them, with defined scope and lifetime. Logs and inspection omit sensitive payloads by default. Data already sent cannot be revoked retroactively. TLS does not prevent the authorized receiver from reading the content.

## Revocation

Revoking a capability blocks new calls; revoking a handle prevents future mediated operations. Do not claim revocation of already-copied bytes/FDs. In-flight operations and external writes follow the [LIFECYCLE](LIFECYCLE.md) and [REMOTE](REMOTE.md) contracts.

A network listener is opt-in. Local installation must work without open external ports. Provisioning, rotation, and revocation of remote identity need an operational procedure before M5.

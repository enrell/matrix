//! `matrix-proto` — executable Matrix Component Protocol v0.1 spec.
//!
//! M2.1: framing com prefixo u32 + JSON UTF-8, envelope com schemas por tipo,
//! version negotiation, idempotent id window, stable errors, and
//! explicit distinction from the legacy protocol (JSON line with `verb`).
//!
//! Finite bounds on every parse (C12): size before allocating, unique
//! uniqueness, max depth, bounded id retention.

pub mod classify;
pub mod dependency;
pub mod envelope;
pub mod error;
pub mod frame;
pub mod handshake;
pub mod remote;

pub use classify::{classify, WireKind};
pub use dependency::{
    validate_body as validate_dependency_body, DEPENDENCY_CALLS_1, DUPLICATE_REQUEST,
    INVALID_PARENT, UNKNOWN_REQUEST, UNSUPPORTED_FEATURE,
};
pub use envelope::{
    parse_frame_payload, scan_raw, validate_envelope, Envelope, MAX_DEPTH, PROTOCOL_ID,
    PROTOCOL_VERSION, TYPES,
};
pub use error::{kernel_code_to_wire, WireError};
pub use frame::{encode, read_frame, read_len, split_frame, write_frame, write_frame_deadline, FrameError, DEFAULT_MAX_FRAME, LEN_PREFIX};
pub use handshake::{hello_offer, negotiate, shrink_timeout_ms, IdVerdict, IdWindow, Limits, Welcome, SUPPORTED_FEATURES, SUPPORTED_VERSIONS};
pub use remote::{
    validate_body as validate_remote_body, validate_envelope as validate_remote_envelope,
    REMOTE_CALLS_1, REMOTE_EVENTS_1, REMOTE_OPS_1, REMOTE_PROFILE, REMOTE_PROTOCOL_ID,
    REMOTE_PROTOCOL_VERSION, REMOTE_STREAMS_1,
};

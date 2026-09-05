//! `matrix-proto` — especificação executável do Matrix Component Protocol v0.1.
//!
//! M2.1: framing com prefixo u32 + JSON UTF-8, envelope com schemas por tipo,
//! negociação de versão, janela de IDs idempotente, erros estáveis e
//! distinção explícita do protocolo legado (linha JSON com `verb`).
//!
//! Limites finitos em todo parse (C12): tamanho antes de alocar, chaves
//! únicas, profundidade máxima, retenção limitada de IDs.

pub mod classify;
pub mod envelope;
pub mod error;
pub mod frame;
pub mod handshake;

pub use classify::{classify, WireKind};
pub use envelope::{
    parse_frame_payload, scan_raw, validate_envelope, Envelope, MAX_DEPTH, PROTOCOL_ID,
    PROTOCOL_VERSION, TYPES,
};
pub use error::{kernel_code_to_wire, WireError};
pub use frame::{encode, read_frame, read_len, split_frame, write_frame, FrameError, DEFAULT_MAX_FRAME, LEN_PREFIX};
pub use handshake::{hello_offer, negotiate, shrink_timeout_ms, IdVerdict, IdWindow, Limits, Welcome, SUPPORTED_VERSIONS};

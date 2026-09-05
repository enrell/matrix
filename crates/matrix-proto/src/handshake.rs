//! Negociação de versão, limites e deduplicação de IDs (M2.1).
//!
//! - `hello` anuncia versões suportadas + `max_frame` + limites;
//!   `welcome` fecha a maior versão comum + `session_id`; sem interseção,
//!   `reject` com `unsupported-version`.
//! - Autenticação precede autorização: sem sessão negociada, nada além de
//!   `hello` é aceito (o host aplica; aqui vai o predicado).
//! - Repetição idempotente: `IdWindow` guarda IDs vistos na retenção; mesmo
//!   id + mesmo conteúdo = repetição segura; mesmo id + conteúdo divergente
//!   = erro (nunca reexecuta efeitos).

use crate::envelope::{Envelope, PROTOCOL_VERSION};
use crate::error::{WireError, UNSUPPORTED_VERSION};
use std::collections::{HashMap, VecDeque};

/// Versões que este lado fala, da preferida à aceitável.
pub const SUPPORTED_VERSIONS: &[&str] = &[PROTOCOL_VERSION];

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_frame: usize,
    pub max_calls: usize,
    pub max_queue_bytes: usize,
    pub max_streams: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: crate::frame::DEFAULT_MAX_FRAME,
            max_calls: 64,
            max_queue_bytes: 4 * 1024 * 1024,
            max_streams: 16,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Welcome {
    pub version: String,
    pub session_id: String,
    pub max_frame: usize,
    pub limits: Limits,
}

/// Negocia a partir de um `hello` já validado. `offered` = body.versions.
pub fn negotiate(
    offered: &[String],
    want_frame: usize,
    session_id: String,
) -> Result<Welcome, WireError> {
    let version = SUPPORTED_VERSIONS
        .iter()
        .find(|v| offered.iter().any(|o| o == **v))
        .ok_or_else(|| {
            WireError::new(UNSUPPORTED_VERSION, "handshake").with_details(serde_json::json!({
                "offered": offered,
                "supported": SUPPORTED_VERSIONS,
            }))
        })?;
    let max_frame = want_frame.min(crate::frame::DEFAULT_MAX_FRAME).max(1024);
    Ok(Welcome {
        version: version.to_string(),
        session_id,
        max_frame,
        limits: Limits { max_frame, ..Limits::default() },
    })
}

/// Extrai `versions` + `max_frame` do body de um hello validado.
pub fn hello_offer(env: &Envelope) -> Result<(Vec<String>, usize), WireError> {
    if env.ty != "hello" {
        return Err(WireError::new(crate::error::INVALID_MESSAGE, "handshake")
            .with_details(serde_json::json!({"reason": "not a hello"})));
    }
    let versions = env
        .body
        .get("versions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            WireError::new(crate::error::INVALID_MESSAGE, "handshake")
                .with_details(serde_json::json!({"reason": "versions must be array"}))
        })?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    if versions.is_empty() {
        return Err(WireError::new(crate::error::INVALID_MESSAGE, "handshake")
            .with_details(serde_json::json!({"reason": "empty versions"})));
    }
    let max_frame = env.body.get("max_frame").and_then(|v| v.as_u64()).unwrap_or(1024) as usize;
    Ok((versions, max_frame))
}

/// Janela de IDs vistos (deduplicação com retenção limitada).
pub struct IdWindow {
    cap: usize,
    seen: HashMap<String, u64>,
    order: VecDeque<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdVerdict {
    /// Primeira vez: processar e guardar o hash do conteúdo.
    New,
    /// Repetição idêntica dentro da retenção: responder o terminal guardado,
    /// sem reexecutar.
    DuplicateSame,
    /// Mesmo id, conteúdo divergente: erro, sem executar.
    DuplicateDivergent,
}

impl IdWindow {
    pub fn new(cap: usize) -> Self {
        Self { cap: cap.max(1), seen: HashMap::new(), order: VecDeque::new() }
    }

    fn hash_content(canonical: &str) -> u64 {
        // FNV-1a 64: suficiente para detecção de divergência na retenção.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in canonical.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    pub fn check(&mut self, id: &str, content_canonical: &str) -> IdVerdict {
        let h = Self::hash_content(content_canonical);
        match self.seen.get(id) {
            None => {
                if self.order.len() >= self.cap {
                    if let Some(old) = self.order.pop_front() {
                        self.seen.remove(&old);
                    }
                }
                self.seen.insert(id.to_string(), h);
                self.order.push_back(id.to_string());
                IdVerdict::New
            }
            Some(prev) if *prev == h => IdVerdict::DuplicateSame,
            Some(_) => IdVerdict::DuplicateDivergent,
        }
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }
}

/// Reduz `timeout_ms` restante pelo decorrido (nunca negativo).
pub fn shrink_timeout_ms(remaining_ms: u64, elapsed_ms: u64) -> u64 {
    remaining_ms.saturating_sub(elapsed_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_common_or_rejects() {
        let w = negotiate(&["0.1".to_string()], 1 << 20, "s".into()).unwrap();
        assert_eq!(w.version, "0.1");
        assert!(negotiate(&["9.9".to_string()], 1 << 20, "s".into()).is_err());
        let w = negotiate(&["9.9".to_string(), "0.1".to_string()], 512, "s".into()).unwrap();
        assert_eq!(w.version, "0.1");
        assert!(w.max_frame >= 1024, "piso de sanidade");
    }

    #[test]
    fn id_window_semantics() {
        let mut win = IdWindow::new(2);
        assert_eq!(win.check("a", "{}"), IdVerdict::New);
        assert_eq!(win.check("a", "{}"), IdVerdict::DuplicateSame);
        assert_eq!(win.check("a", "{\"x\":1}"), IdVerdict::DuplicateDivergent);
        win.check("b", "{}");
        win.check("c", "{}"); // despeja "a"
        assert_eq!(win.check("a", "{}"), IdVerdict::New);
    }

    #[test]
    fn timeout_shrinks() {
        assert_eq!(shrink_timeout_ms(5000, 1200), 3800);
        assert_eq!(shrink_timeout_ms(100, 5000), 0);
    }
}

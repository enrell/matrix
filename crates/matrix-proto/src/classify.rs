//! Distinção legado × novo (M2.1).
//!
//! O JSON por linha do scaffold (`{"v":1,"verb":...}`) é um protocolo legado
//! separado: o listener novo nunca infere compatibilidade por ambos terem
//! um campo `v`. A classificação exige o marcador de cada perfil.

use crate::envelope::PROTOCOL_ID;
use crate::frame::LEN_PREFIX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireKind {
    /// Frame novo: prefixo u32 + JSON com `protocol: matrix.component`.
    MatrixFrame,
    /// Linha legada: JSON com `verb` terminado em `\n`.
    LegacyLine,
    /// Insuficiente ou irreconhecível (ainda).
    Unknown,
}

/// Classifica os primeiros bytes disponíveis sem consumir.
pub fn classify(buf: &[u8], max_frame: usize) -> WireKind {
    if let Some(kind) = classify_new(buf, max_frame) {
        return kind;
    }
    if classify_legacy(buf) {
        return WireKind::LegacyLine;
    }
    WireKind::Unknown
}

fn classify_new(buf: &[u8], max_frame: usize) -> Option<WireKind> {
    if buf.len() < LEN_PREFIX {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 || len > max_frame || buf.len() < LEN_PREFIX + len {
        return None;
    }
    let payload = &buf[LEN_PREFIX..LEN_PREFIX + len];
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    if v.get("protocol").and_then(|p| p.as_str()) == Some(PROTOCOL_ID) {
        Some(WireKind::MatrixFrame)
    } else {
        None
    }
}

fn classify_legacy(buf: &[u8]) -> bool {
    let Some(nl) = buf.iter().position(|b| *b == b'\n') else { return false };
    let line = &buf[..nl];
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) else { return false };
    v.get("verb").and_then(|x| x.as_str()).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_new_from_legacy() {
        let payload = serde_json::json!({"protocol": PROTOCOL_ID, "version": "0.1"}).to_string();
        let mut f = (payload.len() as u32).to_be_bytes().to_vec();
        f.extend_from_slice(payload.as_bytes());
        assert_eq!(classify(&f, 1 << 20), WireKind::MatrixFrame);

        let legacy = b"{\"v\":1,\"verb\":\"invoke\",\"extra\":{}}\n";
        assert_eq!(classify(legacy, 1 << 20), WireKind::LegacyLine);

        // Frame novo sem marcador de protocolo não é confundido com legado.
        let other = serde_json::json!({"v": 1, "verb": "x"}).to_string();
        let mut g = (other.len() as u32).to_be_bytes().to_vec();
        g.extend_from_slice(other.as_bytes());
        assert_eq!(classify(&g, 1 << 20), WireKind::Unknown);

        assert_eq!(classify(b"\x00", 1 << 20), WireKind::Unknown);
        assert_eq!(classify(b"not json\n", 1 << 20), WireKind::Unknown);
    }
}

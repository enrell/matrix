//! Framing do perfil inicial (M2.1, cf. `docs/PROTOCOL.md` §5).
//!
//! Mensagens JSON UTF-8 com prefixo de comprimento u32 big-endian.
//! Limite padrão de frame: 1 MiB; limites menores podem ser negociados.
//! O tamanho é validado ANTES de alocar o payload (C12).

use std::io::{Read, Write};

/// Limite padrão de frame: 1 MiB.
pub const DEFAULT_MAX_FRAME: usize = 1024 * 1024;
/// Prefixo de comprimento: u32 big-endian.
pub const LEN_PREFIX: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Payload declara tamanho acima do limite (sem alocar).
    TooLarge { declared: u64, max: usize },
    /// Stream terminou no meio do frame.
    Truncated { want: usize, got: usize },
    /// I/O subjacente.
    Io(String),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::TooLarge { declared, max } => {
                write!(f, "frame declares {} bytes, max is {}", declared, max)
            }
            FrameError::Truncated { want, got } => {
                write!(f, "truncated frame: want {} bytes, got {}", want, got)
            }
            FrameError::Io(e) => write!(f, "io: {}", e),
        }
    }
}

impl std::error::Error for FrameError {}

/// Codifica um payload num frame completo (prefixo + bytes).
pub fn encode(payload: &[u8], max_frame: usize) -> Result<Vec<u8>, FrameError> {
    if payload.len() > max_frame {
        return Err(FrameError::TooLarge { declared: payload.len() as u64, max: max_frame });
    }
    if payload.len() > u32::MAX as usize {
        return Err(FrameError::TooLarge { declared: payload.len() as u64, max: max_frame });
    }
    let mut out = Vec::with_capacity(LEN_PREFIX + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Lê o prefixo de 4 bytes. `Ok(None)` = EOF limpo antes de qualquer byte.
pub fn read_len<R: Read>(r: &mut R) -> Result<Option<u32>, FrameError> {
    let mut hdr = [0u8; LEN_PREFIX];
    let mut got = 0;
    while got < LEN_PREFIX {
        match r.read(&mut hdr[got..]) {
            Ok(0) => {
                if got == 0 {
                    return Ok(None);
                }
                return Err(FrameError::Truncated { want: LEN_PREFIX, got });
            }
            Ok(n) => got += n,
            Err(e) => return Err(FrameError::Io(e.to_string())),
        }
    }
    Ok(Some(u32::from_be_bytes(hdr)))
}

/// Lê um frame após validar o tamanho declarado (sem super-alocar).
pub fn read_frame<R: Read>(r: &mut R, max_frame: usize) -> Result<Option<Vec<u8>>, FrameError> {
    let Some(len) = read_len(r)? else { return Ok(None) };
    if len as u64 > max_frame as u64 {
        return Err(FrameError::TooLarge { declared: len as u64, max: max_frame });
    }
    let mut buf = vec![0u8; len as usize];
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => return Err(FrameError::Truncated { want: buf.len(), got }),
            Ok(n) => got += n,
            Err(e) => return Err(FrameError::Io(e.to_string())),
        }
    }
    Ok(Some(buf))
}

/// Escreve um frame completo.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> Result<(), FrameError> {
    w.write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(|e| FrameError::Io(e.to_string()))?;
    w.write_all(payload).map_err(|e| FrameError::Io(e.to_string()))?;
    w.flush().map_err(|e| FrameError::Io(e.to_string()))?;
    Ok(())
}

/// Extrai o próximo frame de um buffer acumulado. Retorna o frame e o
/// restante, ou `None` se incompleto. Valida o tamanho antes de fatiar.
pub fn split_frame(buf: &[u8], max_frame: usize) -> Result<Option<(&[u8], &[u8])>, FrameError> {
    if buf.len() < LEN_PREFIX {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > max_frame {
        return Err(FrameError::TooLarge { declared: len as u64, max: max_frame });
    }
    if buf.len() < LEN_PREFIX + len {
        return Ok(None);
    }
    Ok(Some((&buf[LEN_PREFIX..LEN_PREFIX + len], &buf[LEN_PREFIX + len..])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = encode(b"{}", 1024).unwrap();
        let (body, rest) = split_frame(&f, 1024).unwrap().unwrap();
        assert_eq!(body, b"{}");
        assert!(rest.is_empty());
    }

    #[test]
    fn oversize_rejected_before_alloc() {
        assert!(matches!(
            encode(&vec![0u8; 10], 9),
            Err(FrameError::TooLarge { .. })
        ));
        let mut hdr = (100u32).to_be_bytes().to_vec();
        hdr.extend_from_slice(b"short");
        assert!(matches!(split_frame(&hdr, 50), Err(FrameError::TooLarge { .. })));
    }

    #[test]
    fn incomplete_is_none_not_error() {
        assert!(split_frame(b"\x00\x00", 1024).unwrap().is_none());
        let mut f = encode(b"hello", 1024).unwrap();
        f.truncate(6);
        assert!(split_frame(&f, 1024).unwrap().is_none());
    }
}

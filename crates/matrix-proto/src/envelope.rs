//! Envelope `matrix.component` v0.1 e schemas por tipo (M2.1).
//!
//! - `protocol` sempre `"matrix.component"`; `version` `"0.1"` (negociada).
//! - Inteiros de sequência/geração/instância são strings decimais.
//! - Validação em duas camadas: varredura crua (JSON válido, sem chaves
//!   duplicadas, profundidade limitada) e schema por tipo (campos
//!   obrigatórios). Tipo desconhecido é erro; extensões opcionais
//!   desconhecidas só passam se o schema permitir (aqui: `extensions`).

use crate::error::{WireError, INVALID_MESSAGE};
use serde_json::Value;

pub const PROTOCOL_ID: &str = "matrix.component";
pub const PROTOCOL_VERSION: &str = "0.1";
/// Profundidade máxima de aninhamento (C12).
pub const MAX_DEPTH: usize = 32;

/// Tipos de mensagem das famílias do documento.
pub const TYPES: &[&str] = &[
    "hello", "welcome", "reject",
    "component.register", "registered",
    "lifecycle.prepare", "lifecycle.activate", "lifecycle.quiesce", "lifecycle.dispose",
    "lifecycle.result", "capability.changed",
    "resource.acquire", "resource.release", "resource.result",
    "call.open", "call.accepted", "call.result", "call.error",
    "call.cancel", "call.cancel.result",
    "stream.data", "stream.credit", "stream.end",
    "session.heartbeat", "session.renew", "session.close",
    "inspect.request", "inspect.result",
];

/// Campos obrigatórios de topo por tipo (além de protocol/version/type).
/// `body` é obrigatório salvo indicação; campos de identidade seguem a família.
fn required_top(ty: &str) -> Option<(&'static [&'static str], bool)> {
    Some(match ty {
        "hello" => (&["message_id", "body"], false),
        "welcome" => (&["message_id", "session_id", "body"], false),
        "reject" => (&["message_id", "body"], false),
        "component.register" => (&["message_id", "session_id", "body"], false),
        "registered" => (&["message_id", "session_id", "instance_id", "generation", "body"], false),
        "lifecycle.prepare" | "lifecycle.activate" | "lifecycle.quiesce" | "lifecycle.dispose" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "lifecycle.result" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "capability.changed" => (&["message_id", "session_id", "body"], false),
        "resource.acquire" | "resource.release" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "resource.result" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "call.open" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "call.accepted" | "call.result" | "call.error" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "call.cancel" | "call.cancel.result" => {
            (&["message_id", "session_id", "instance_id", "generation", "request_id", "body"], false)
        }
        "stream.data" | "stream.credit" | "stream.end" => {
            (&["message_id", "session_id", "instance_id", "generation", "body"], false)
        }
        "session.heartbeat" | "session.renew" | "session.close" => {
            (&["message_id", "session_id"], true)
        }
        "inspect.request" => (&["message_id", "body"], false),
        "inspect.result" => (&["message_id", "body"], false),
        _ => return None,
    })
}

/// Campos obrigatórios do `body` por tipo.
fn required_body(ty: &str) -> &'static [&'static str] {
    match ty {
        "hello" => &["versions", "max_frame", "client"],
        "welcome" => &["version", "max_frame", "limits"],
        "reject" => &["code", "reason"],
        "component.register" => &["manifest"],
        "registered" => &["logical"],
        "lifecycle.prepare" | "lifecycle.activate" => &["operation_id", "manifest", "bindings"],
        "lifecycle.quiesce" | "lifecycle.dispose" => &["operation_id", "deadline_ms"],
        "lifecycle.result" => &["operation_id", "status"],
        "capability.changed" => &["revision", "change"],
        "resource.acquire" => &["operation_id", "kind", "label"],
        "resource.release" => &["operation_id", "handle"],
        "resource.result" => &["operation_id", "status"],
        "call.open" => &["ticket", "capability", "input"],
        "call.accepted" => &["ticket"],
        "call.result" => &["ticket", "status"],
        "call.error" => &["ticket", "error"],
        "call.cancel" => &["ticket", "reason"],
        "call.cancel.result" => &["ticket", "status"],
        "stream.data" => &["stream_id", "seq", "payload"],
        "stream.credit" => &["stream_id", "bytes"],
        "stream.end" => &["stream_id", "status"],
        "inspect.request" => &["query"],
        "inspect.result" => &["result"],
        _ => &[],
    }
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub ty: String,
    pub message_id: String,
    pub session_id: Option<String>,
    pub instance_id: Option<String>,
    pub generation: Option<u64>,
    pub request_id: Option<String>,
    pub body: Value,
}

/// Varredura crua: JSON bem-formado, sem chaves duplicadas em objetos,
/// profundidade ≤ `MAX_DEPTH`. Retorna o valor parseado.
pub fn scan_raw(raw: &[u8]) -> Result<Value, WireError> {
    let s = std::str::from_utf8(raw)
        .map_err(|e| WireError::new(INVALID_MESSAGE, "framing").with_details(serde_json::json!({"reason": e.to_string()})))?;
    scan_structure(s)?;
    serde_json::from_str(s)
        .map_err(|e| WireError::new(INVALID_MESSAGE, "framing").with_details(serde_json::json!({"reason": e.to_string()})))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    Obj,
    Arr,
}

struct Frame {
    ctx: Ctx,
    keys: std::collections::HashSet<String>,
    expect_key: bool,
}

/// Scanner consciente de strings/escapes que impõe chaves únicas e profundidade.
fn scan_structure(s: &str) -> Result<(), WireError> {
    let bad = |reason: &str| WireError::new(INVALID_MESSAGE, "framing").with_details(serde_json::json!({"reason": reason}));
    let b = s.as_bytes();
    let mut i = 0;
    let mut stack: Vec<Frame> = vec![];
    let mut depth = 0usize;
    let mut root_seen = false;

    let skip_ws = |i: &mut usize| {
        while *i < b.len() && (b[*i] == b' ' || b[*i] == b'\t' || b[*i] == b'\n' || b[*i] == b'\r') {
            *i += 1;
        }
    };
    // Lê string JSON a partir de `i` (que aponta para `"`), retorna conteúdo.
    fn read_string(s: &str, b: &[u8], i: &mut usize) -> Result<String, ()> {
        *i += 1; // abre aspas
        let mut out = String::new();
        loop {
            if *i >= b.len() {
                return Err(());
            }
            match b[*i] {
                b'"' => {
                    *i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    *i += 1;
                    if *i >= b.len() {
                        return Err(());
                    }
                    let e = b[*i];
                    *i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\x08'),
                        b'f' => out.push('\x0c'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            if *i + 4 > b.len() {
                                return Err(());
                            }
                            let hex = std::str::from_utf8(&b[*i..*i + 4]).map_err(|_| ())?;
                            let cp = u32::from_str_radix(hex, 16).map_err(|_| ())?;
                            out.push(char::from_u32(cp).ok_or(())?);
                            *i += 4;
                        }
                        _ => return Err(()),
                    }
                }
                _ => {
                    // UTF-8 multibyte: avança pelo char completo.
                    let ch = s[*i..].chars().next().ok_or(())?;
                    out.push(ch);
                    *i += ch.len_utf8();
                }
            }
        }
    }
    fn skip_literal(b: &[u8], i: &mut usize) -> Result<(), ()> {
        let rest = &b[*i..];
        for lit in ["true", "false", "null"] {
            if rest.starts_with(lit.as_bytes()) {
                *i += lit.len();
                return Ok(());
            }
        }
        // número
        let mut j = *i;
        if j < b.len() && (b[j] == b'-') {
            j += 1;
        }
        let mut any = false;
        while j < b.len() && (b[j].is_ascii_digit() || b[j] == b'.' || b[j] == b'e' || b[j] == b'E' || b[j] == b'+' || b[j] == b'-') {
            j += 1;
            any = true;
        }
        if !any {
            return Err(());
        }
        *i = j;
        Ok(())
    }

    skip_ws(&mut i);
    loop {
        skip_ws(&mut i);
        if i >= b.len() {
            break;
        }
        // Valor esperado aqui?
        let in_obj_expect_key = stack.last().map(|f| f.ctx == Ctx::Obj && f.expect_key).unwrap_or(false);
        match b[i] {
            b'{' => {
                depth += 1;
                if depth > MAX_DEPTH {
                    return Err(bad("max depth exceeded"));
                }
                if stack.last().map(|f| f.ctx == Ctx::Arr).unwrap_or(false) {
                    // valor em array: ok, nada a marcar
                } else if stack.is_empty() {
                    if root_seen {
                        return Err(bad("trailing data"));
                    }
                    root_seen = true;
                } else if in_obj_expect_key {
                    // onde se esperava chave, '{' é ilegal (chave é string).
                    return Err(bad("unexpected '{'"));
                } else {
                    stack.last_mut().unwrap().expect_key = false;
                }
                stack.push(Frame { ctx: Ctx::Obj, keys: Default::default(), expect_key: true });
                // objeto vazio? marca e segue para '}'
                i += 1;
            }
            b'[' => {
                depth += 1;
                if depth > MAX_DEPTH {
                    return Err(bad("max depth exceeded"));
                }
                if stack.is_empty() {
                    if root_seen {
                        return Err(bad("trailing data"));
                    }
                    root_seen = true;
                } else if in_obj_expect_key {
                    return Err(bad("unexpected '['"));
                } else if stack.last().map(|f| f.ctx == Ctx::Obj).unwrap_or(false) {
                    stack.last_mut().unwrap().expect_key = false;
                }
                stack.push(Frame { ctx: Ctx::Arr, keys: Default::default(), expect_key: false });
                i += 1;
            }
            b'}' | b']' => {
                let want = if b[i] == b'}' { Ctx::Obj } else { Ctx::Arr };
                match stack.pop() {
                    Some(f) if f.ctx == want => {
                        depth -= 1;
                        i += 1;
                        // após fechar, contexto pai espera ',' ou fechamento
                        if let Some(p) = stack.last_mut() {
                            if p.ctx == Ctx::Obj {
                                p.expect_key = false; // valor concluído; próxima deve ser ',' ou '}'
                            }
                        }
                    }
                    _ => return Err(bad("unbalanced close")),
                }
            }
            b'"' => {
                let st = read_string(s, b, &mut i).map_err(|_| bad("bad string"))?;
                skip_ws(&mut i);
                if let Some(top) = stack.last_mut() {
                    if top.ctx == Ctx::Obj && top.expect_key {
                        if i < b.len() && b[i] == b':' {
                            if !top.keys.insert(st) {
                                return Err(bad("duplicate key"));
                            }
                            top.expect_key = false;
                            i += 1; // consome ':'
                            continue;
                        } else {
                            return Err(bad("expected ':'"));
                        }
                    }
                } else if !root_seen {
                    root_seen = true;
                    // string raiz: ok como valor, mas envelope exige objeto (schema checa)
                }
                // valor string em array/objeto-valor/array-raiz: segue
            }
            b',' => {
                i += 1;
                if let Some(top) = stack.last_mut() {
                    if top.ctx == Ctx::Obj {
                        top.expect_key = true;
                    }
                } else {
                    return Err(bad("stray ','"));
                }
            }
            b':' => return Err(bad("stray ':'")),
            _ => {
                skip_literal(b, &mut i).map_err(|_| bad("bad literal"))?;
                if let Some(top) = stack.last_mut() {
                    if top.ctx == Ctx::Obj && top.expect_key {
                        return Err(bad("expected string key"));
                    }
                    if top.ctx == Ctx::Obj {
                        top.expect_key = false;
                    }
                } else if !root_seen {
                    root_seen = true;
                }
            }
        }
    }
    if !stack.is_empty() || !root_seen {
        return Err(bad("incomplete document"));
    }
    Ok(())
}

fn decimal_u64(v: &Value, field: &str) -> Result<u64, WireError> {
    let bad = || {
        WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"field": field, "reason": "decimal string expected"}))
    };
    match v {
        Value::String(s) => s.parse::<u64>().map_err(|_| bad()),
        _ => Err(bad()),
    }
}

/// Valida envelope completo (top + body por tipo).
pub fn validate_envelope(v: &Value) -> Result<Envelope, WireError> {
    let obj = v.as_object().ok_or_else(|| {
        WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"reason": "object expected"}))
    })?;
    let get = |f: &str| {
        obj.get(f).ok_or_else(|| {
            WireError::new(INVALID_MESSAGE, "envelope")
                .with_details(serde_json::json!({"missing": f}))
        })
    };
    if get("protocol")?.as_str() != Some(PROTOCOL_ID) {
        return Err(WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"reason": "unknown protocol"})));
    }
    let version = get("version")?.as_str().ok_or_else(|| {
        WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"reason": "version must be string"}))
    })?;
    if version != PROTOCOL_VERSION {
        return Err(WireError::new(crate::error::UNSUPPORTED_VERSION, "handshake")
            .with_details(serde_json::json!({"got": version, "want": PROTOCOL_VERSION})));
    }
    let ty = get("type")?.as_str().ok_or_else(|| {
        WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"reason": "type must be string"}))
    })?;
    let (req_top, body_optional) = required_top(ty).ok_or_else(|| {
        WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"unknown_type": ty}))
    })?;
    for f in req_top {
        get(f)?;
    }
    let message_id = get("message_id")?.as_str().ok_or_else(|| {
        WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"reason": "message_id must be string"}))
    })?.to_string();
    if message_id.is_empty() {
        return Err(WireError::new(INVALID_MESSAGE, "envelope")
            .with_details(serde_json::json!({"reason": "empty message_id"})));
    }
    let session_id = obj.get("session_id").and_then(|v| v.as_str()).map(|s| s.to_string());
    let instance_id = obj.get("instance_id").and_then(|v| v.as_str()).map(|s| s.to_string());
    let generation = obj.get("generation").map(|v| decimal_u64(v, "generation")).transpose()?;
    // instance_id, quando presente, também é decimal.
    if let Some(iid) = obj.get("instance_id") {
        decimal_u64(iid, "instance_id")?;
    }
    let request_id = obj.get("request_id").and_then(|v| v.as_str()).map(|s| s.to_string());
    let body = if body_optional {
        obj.get("body").cloned().unwrap_or(Value::Null)
    } else {
        get("body")?.clone()
    };
    if !body_optional {
        let bobj = body.as_object().ok_or_else(|| {
            WireError::new(INVALID_MESSAGE, "envelope")
                .with_details(serde_json::json!({"reason": "body must be object"}))
        })?;
        for f in required_body(ty) {
            if !bobj.contains_key(*f) {
                return Err(WireError::new(INVALID_MESSAGE, "envelope").with_details(
                    serde_json::json!({"type": ty, "missing_body": f}),
                ));
            }
        }
    }
    Ok(Envelope { ty: ty.to_string(), message_id, session_id, instance_id, generation, request_id, body })
}

/// Parse completo de um payload de frame: varredura + schema.
pub fn parse_frame_payload(raw: &[u8]) -> Result<Envelope, WireError> {
    let v = scan_raw(raw)?;
    validate_envelope(&v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env(ty: &str, extra: Value) -> Value {
        let mut o = serde_json::Map::new();
        o.insert("protocol".into(), json!(PROTOCOL_ID));
        o.insert("version".into(), json!(PROTOCOL_VERSION));
        o.insert("type".into(), json!(ty));
        if let Value::Object(m) = extra {
            for (k, v) in m {
                o.insert(k, v);
            }
        }
        Value::Object(o)
    }

    #[test]
    fn valid_call_open() {
        let v = env("call.open", json!({
            "message_id": "m1", "session_id": "s1", "instance_id": "7",
            "generation": "3", "request_id": "r1",
            "body": {"ticket": "tkt-9", "capability": "search.query@1", "input": {}}
        }));
        let e = validate_envelope(&v).unwrap();
        assert_eq!(e.generation, Some(3));
    }

    #[test]
    fn rejects_unknown_type_and_missing_fields() {
        let v = env("frobnicate", json!({"message_id": "m"}));
        assert!(validate_envelope(&v).is_err());
        let v = env("call.open", json!({"message_id": "m"}));
        assert!(validate_envelope(&v).is_err());
    }

    #[test]
    fn rejects_dup_keys_and_depth() {
        let raw = br#"{"protocol":"matrix.component","version":"0.1","type":"hello","type":"hello","message_id":"m","body":{"versions":["0.1"],"max_frame":1024,"client":"x"}}"#;
        assert!(scan_raw(raw).is_err());
        let mut deep = String::from("{\"a\":");
        for _ in 0..40 {
            deep.push_str("{\"a\":");
        }
        deep.push('1');
        for _ in 0..41 {
            deep.push('}');
        }
        assert!(scan_raw(deep.as_bytes()).is_err());
    }

    #[test]
    fn rejects_bad_generation_and_protocol() {
        let v = env("registered", json!({
            "message_id": "m", "session_id": "s", "instance_id": "x",
            "generation": "3", "body": {"logical": "p"}
        }));
        assert!(validate_envelope(&v).is_err());
        let mut v = env("hello", json!({"message_id": "m", "body": {"versions": ["0.1"], "max_frame": 1, "client": "c"}}));
        v["protocol"] = json!("other");
        assert!(validate_envelope(&v).is_err());
    }
}

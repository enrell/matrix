//! SDK Rust para componentes externos (M2.4).
//!
//! Fala `matrix.component` v0.1 com o host local: handshake, registro,
//! ativação e atendimento de chamadas com cancelamento cooperativo.
//! O kernel continua decidindo composição e lifecycle; o SDK só traduz
//! o contrato para Rust idiomático (cf. `docs/SDK.md`).
//!
//! ```no_run
//! use matrix_component::{CallCtx, CallOutcome, Component, Handler};
//! use serde_json::{json, Value};
//! use std::sync::atomic::AtomicBool;
//!
//! struct Echo;
//! impl Handler for Echo {
//!     fn on_call(&self, _ctx: &CallCtx, _ticket: &str, _cap: &str, input: &Value, _cancel: &AtomicBool) -> CallOutcome {
//!         CallOutcome::Ok(json!({"echo": input}))
//!     }
//! }
//!
//! let comp = Component::connect(std::path::Path::new("/run/host.sock"), "echo").unwrap();
//! comp.serve(Echo).unwrap();
//! ```

use matrix_proto::{
    encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{
    atomic::AtomicBool,
    Arc, Mutex,
};
use std::time::Duration;

/// Resultado de negócio da chamada (código de erro preservado no fio).
#[derive(Debug, Clone)]
pub enum CallOutcome {
    Ok(Value),
    Err { code: String, message: String },
}

/// Contexto de uma chamada: sessão amarrada + envio de streams.
#[derive(Clone)]
pub struct CallCtx {
    writer: Arc<Mutex<UnixStream>>,
    pub session_id: String,
    pub instance_id: String,
    pub generation: u64,
    pub max_frame: usize,
}

impl CallCtx {
    /// Envia um quadro de stream (uma via; crédito é gerido pelo host).
    pub fn send_stream(&self, stream_id: &str, seq: u64, payload: &str) -> Result<(), String> {
        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.data",
            "message_id": format!("m-{}-{}", stream_id, seq),
            "session_id": self.session_id,
            "instance_id": self.instance_id,
            "generation": self.generation.to_string(),
            "body": {"stream_id": stream_id, "seq": seq.to_string(), "payload": payload},
        });
        let raw = serde_json::to_vec(&msg).map_err(|e| e.to_string())?;
        let frame = encode(&raw, self.max_frame).map_err(|e| e.to_string())?;
        let mut w = self.writer.lock().map_err(|e| e.to_string())?;
        w.write_all(&frame).map_err(|e| e.to_string())?;
        w.flush().map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// Lógica do componente. `on_call` roda numa thread por chamada; `cancel`
/// é sinalizado em `call.cancel` ou na saída. `on_cancel` é opcional
/// (observabilidade; o worker deve cooperar via `cancel`).
pub trait Handler: Send + Sync + 'static {
    fn on_call(
        &self,
        ctx: &CallCtx,
        ticket: &str,
        cap: &str,
        input: &Value,
        cancel: &AtomicBool,
    ) -> CallOutcome;

    fn on_cancel(&self, _ticket: &str) {}
}

struct CallCtl {
    cancel: Arc<AtomicBool>,
}

/// Componente conectado: sessão negociada e ativada.
pub struct Component {
    writer: Arc<Mutex<UnixStream>>,
    reader: UnixStream,
    session_id: String,
    instance_id: String,
    generation: u64,
    max_frame: usize,
}

impl Component {
    /// Conecta, negocia versão, registra `logical` e conclui o activate.
    pub fn connect(sock: &Path, logical: &str) -> Result<Self, String> {
        let mut stream =
            UnixStream::connect(sock).map_err(|e| format!("connect: {}", e))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| e.to_string())?;
        let writer = Arc::new(Mutex::new(
            stream.try_clone().map_err(|e| e.to_string())?,
        ));
        let send = |v: &Value, mf: usize| -> Result<(), String> {
            let raw = serde_json::to_vec(v).map_err(|e| e.to_string())?;
            let frame = encode(&raw, mf).map_err(|e| e.to_string())?;
            let mut w = writer.lock().map_err(|e| e.to_string())?;
            w.write_all(&frame).map_err(|e| e.to_string())?;
            w.flush().map_err(|e| e.to_string())?;
            Ok(())
        };
        send(
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "hello",
                "message_id": "h1",
                "body": {"launch_token": std::env::var("MATRIX_LAUNCH_TOKEN").unwrap_or_default(), "versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "matrix-component"},
            }),
            DEFAULT_MAX_FRAME,
        )?;
        let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "sem welcome".to_string())?;
        let welcome =
            parse_frame_payload(&raw).map_err(|e| format!("welcome: {:?}", e))?;
        if welcome.ty != "welcome" {
            return Err(format!("esperava welcome, veio {}", welcome.ty));
        }
        let session_id = welcome.session_id.clone().ok_or("welcome sem sessão")?;
        let max_frame = welcome
            .body
            .get("max_frame")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_MAX_FRAME as u64) as usize;
        send(
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "component.register",
                "message_id": "reg1",
                "session_id": session_id,
                "body": {"manifest": {"id": logical}},
            }),
            max_frame,
        )?;
        let raw = read_frame(&mut stream, max_frame)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "sem registered".to_string())?;
        let reg =
            parse_frame_payload(&raw).map_err(|e| format!("registered: {:?}", e))?;
        if reg.ty != "registered" {
            return Err(format!("registro rejeitado: {:?}", reg.body));
        }
        let instance_id = reg.instance_id.clone().ok_or("registered sem instância")?;
        let generation = reg.generation.ok_or("registered sem geração")?;
        // Aguarda o activate do host e confirma.
        let raw = read_frame(&mut stream, max_frame)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "sem activate".to_string())?;
        let act =
            parse_frame_payload(&raw).map_err(|e| format!("activate: {:?}", e))?;
        if act.ty != "lifecycle.activate" {
            return Err(format!("esperava activate, veio {}", act.ty));
        }
        let op = act.body.get("operation_id").cloned().unwrap_or(json!("op?"));
        send(
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "lifecycle.result",
                "message_id": "lc1",
                "session_id": session_id,
                "instance_id": instance_id,
                "generation": generation.to_string(),
                "request_id": act.request_id,
                "body": {"operation_id": op, "status": "ok", "pending": []},
            }),
            max_frame,
        )?;
        Ok(Self {
            writer,
            reader: stream,
            session_id,
            instance_id,
            generation,
            max_frame,
        })
    }

    /// Atende até EOF/erro, quiesce ou dispose (que responde e retorna).
    /// Consome o componente; `Ok(())` = dispose limpo, `Err` = queda.
    pub fn serve<H: Handler>(self, handler: H) -> Result<(), String> {
        let handler = Arc::new(handler);
        let calls: Arc<Mutex<HashMap<String, CallCtl>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let ctx = CallCtx {
            writer: self.writer.clone(),
            session_id: self.session_id.clone(),
            instance_id: self.instance_id.clone(),
            generation: self.generation,
            max_frame: self.max_frame,
        };
        let mut stream = self.reader;
        let (session_id, instance_id, generation, max_frame) = (
            self.session_id.clone(),
            self.instance_id.clone(),
            self.generation,
            self.max_frame,
        );
        let writer0 = self.writer.clone();
        loop {
            let raw = match read_frame(&mut stream, max_frame) {
                Ok(Some(r)) => r,
                Ok(None) => return Err("eof do host".to_string()),
                Err(e) => return Err(format!("leitura: {}", e)),
            };
            let env = match parse_frame_payload(&raw) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let bound_ok = env.session_id.as_deref() == Some(session_id.as_str())
                && env
                    .instance_id
                    .as_deref()
                    .map(|v| v == instance_id)
                    .unwrap_or(true)
                && env.generation.map(|g| g == generation).unwrap_or(true);
            if !bound_ok {
                continue;
            }
            let req_id = env.request_id.clone();
            match env.ty.as_str() {
                "lifecycle.prepare" | "lifecycle.activate" | "lifecycle.quiesce" => {
                    let op = env
                        .body
                        .get("operation_id")
                        .cloned()
                        .unwrap_or(json!("op?"));
                    send_lifecycle_result(&writer0, &session_id, &instance_id, generation, max_frame, &op, req_id.as_deref());
                }
                "lifecycle.dispose" => {
                    let op = env
                        .body
                        .get("operation_id")
                        .cloned()
                        .unwrap_or(json!("op?"));
                    send_lifecycle_result(&writer0, &session_id, &instance_id, generation, max_frame, &op, req_id.as_deref());
                    return Ok(());
                }
                "call.open" => {
                    let ticket = env
                        .body
                        .get("ticket")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let cap = env
                        .body
                        .get("capability")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let input = env.body.get("input").cloned().unwrap_or(Value::Null);
                    let cancel = Arc::new(AtomicBool::new(false));
                    calls.lock().unwrap().insert(
                        ticket.clone(),
                        CallCtl { cancel: cancel.clone() },
                    );
                    let h = handler.clone();
                    let c = ctx.clone();
                    let calls2 = calls.clone();
                    let w = self.writer.clone();
                    let (sid, iid, gen, mf) = (
                        session_id.clone(),
                        instance_id.clone(),
                        generation,
                        max_frame,
                    );
                    let rid = req_id.clone();
                    std::thread::spawn(move || {
                        let out = h.on_call(&c, &ticket, &cap, &input, &cancel);
                        calls2.lock().unwrap().remove(&ticket);
                        // Tardio após cancel: cala (host já abandonou a espera;
                        // sem sucesso falso nem erro fantasma).
                        if cancel.load(std::sync::atomic::Ordering::SeqCst) {
                            return;
                        }
                        let body = match out {
                            CallOutcome::Ok(output) => {
                                json!({"ticket": ticket, "status": "ok", "output": output})
                            }
                            CallOutcome::Err { code, message } => {
                                json!({"ticket": ticket, "status": "error",
                                       "error": {"code": code, "message": message}})
                            }
                        };
                        send_envelope(
                            &w,
                            "call.result",
                            &format!("m-call-{}", ticket),
                            &sid,
                            &iid,
                            gen,
                            rid.as_deref(),
                            body,
                            mf,
                        );
                    });
                }
                "call.cancel" => {
                    let ticket =
                        env.body.get("ticket").and_then(|v| v.as_str()).unwrap_or("");
                    // Sinaliza o worker (que detém clone do Arc) e avisa o
                    // handler. A entrada sai no fim do worker (idempotente).
                    if let Some(c) = calls.lock().unwrap().get(ticket) {
                        c.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    handler.on_cancel(ticket);
                }
                _ => {}
            }
        }
    }

}

fn send_lifecycle_result(
    writer: &Arc<Mutex<UnixStream>>,
    session_id: &str,
    instance_id: &str,
    generation: u64,
    max_frame: usize,
    operation_id: &Value,
    request_id: Option<&str>,
) {
    send_envelope(
        writer,
        "lifecycle.result",
        &format!("m-lc-{}", operation_id.as_str().unwrap_or("op")),
        session_id,
        instance_id,
        generation,
        request_id,
        json!({"operation_id": operation_id, "status": "ok", "pending": []}),
        max_frame,
    );
}

fn send_envelope(
    writer: &Arc<Mutex<UnixStream>>,
    ty: &str,
    message_id: &str,
    session_id: &str,
    instance_id: &str,
    generation: u64,
    request_id: Option<&str>,
    body: Value,
    max_frame: usize,
) {
    let msg = json!({
        "protocol": matrix_proto::PROTOCOL_ID,
        "version": matrix_proto::PROTOCOL_VERSION,
        "type": ty,
        "message_id": message_id,
        "session_id": session_id,
        "instance_id": instance_id,
        "generation": generation.to_string(),
        "request_id": request_id,
        "body": body,
    });
    if let Ok(raw) = serde_json::to_vec(&msg) {
        if let Ok(frame) = encode(&raw, max_frame) {
            if let Ok(w) = writer.lock() {
                let mut w = w;
                use std::io::Write;
                let _ = w.write_all(&frame);
                let _ = w.flush();
            }
        }
    }
}

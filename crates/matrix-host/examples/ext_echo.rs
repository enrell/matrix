//! Plugin externo de referência (M2.2): eco sobre o protocolo novo.
//!
//! Uso: `ext_echo --matrix-sock <sock> --id <logical>`
//! Comportamento para testes do host:
//! - `call.open` com `input.sleep_ms` dorme de forma abortável (5ms);
//! - `input.fail = "CODE"` responde erro de negócio remoto;
//! - `input.count_file = path`: anexa o ticket por chamada recebida;
//! - `input.mark_cancel = path`: anexa o ticket ao receber `call.cancel`;
//! - `input.flood_stream = {id, chunk, count}`: envia N `stream.data` de
//!   `chunk` bytes e responde ok (exercita a porta de crédito M2.3);
//! - senão responde `output = input`;
//! - `call.cancel` aborta a chamada e responde `call.cancel.result`;
//! - `lifecycle.dispose` responde e sai com 0.

use matrix_proto::{
    encode, parse_frame_payload, read_frame, Envelope, DEFAULT_MAX_FRAME,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

struct CallCtl {
    cancel: Arc<AtomicBool>,
    mark_cancel: Option<String>,
}

fn append_line(path: &str, line: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{}", line);
    }
}

fn send(stream: &Arc<Mutex<UnixStream>>, v: &Value, max_frame: usize) {
    let raw = serde_json::to_vec(v).unwrap();
    let frame = encode(&raw, max_frame).unwrap();
    let mut w = stream.lock().unwrap();
    w.write_all(&frame).unwrap();
    w.flush().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut sock = String::new();
    let mut id = "ext-echo".to_string();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--matrix-sock" => {
                i += 1;
                sock = args.get(i).cloned().unwrap_or_default();
            }
            "--id" => {
                i += 1;
                id = args.get(i).cloned().unwrap_or(id);
            }
            _ => {}
        }
        i += 1;
    }
    if sock.is_empty() {
        eprintln!("usage: ext_echo --matrix-sock <sock> [--id <logical>]");
        std::process::exit(2);
    }
    let mut stream = UnixStream::connect(&sock).expect("connect host");
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let writer = Arc::new(Mutex::new(stream.try_clone().expect("clone")));

    // hello → welcome
    send(
        &writer,
        &json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "hello",
            "message_id": "h1",
            "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "ext-echo"},
        }),
        DEFAULT_MAX_FRAME,
    );
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).expect("welcome frame").expect("welcome");
    let welcome = parse_frame_payload(&raw).expect("welcome válido");
    assert_eq!(welcome.ty, "welcome");
    let session_id = welcome.session_id.clone().unwrap();
    let max_frame = welcome.body.get("max_frame").and_then(|v| v.as_u64()).unwrap_or(DEFAULT_MAX_FRAME as u64) as usize;

    // register → registered
    send(
        &writer,
        &json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "component.register",
            "message_id": "reg1",
            "session_id": session_id,
            "body": {"manifest": {"id": id}},
        }),
        max_frame,
    );
    let raw = read_frame(&mut stream, max_frame).expect("registered frame").expect("registered");
    let reg = parse_frame_payload(&raw).expect("registered válido");
    assert_eq!(reg.ty, "registered");
    let instance_id = reg.instance_id.clone().unwrap();
    let generation = reg.generation.unwrap();

    let calls: Arc<Mutex<HashMap<String, CallCtl>>> = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let raw = match read_frame(&mut stream, max_frame) {
            Ok(Some(r)) => r,
            _ => break, // EOF/erro: host caiu; sai.
        };
        let env = match parse_frame_payload(&raw) {
            Ok(e) => e,
            Err(_) => continue,
        };
        let req_id = env.request_id.clone();
        match env.ty.as_str() {
            "lifecycle.prepare" | "lifecycle.activate" | "lifecycle.quiesce" => {
                let op = env.body.get("operation_id").cloned().unwrap_or(json!("op?"));
                reply_lifecycle(&writer, &session_id, &instance_id, generation, &env.message_id, req_id.as_deref(), op, max_frame);
            }
            "lifecycle.dispose" => {
                let op = env.body.get("operation_id").cloned().unwrap_or(json!("op?"));
                reply_lifecycle(&writer, &session_id, &instance_id, generation, &env.message_id, req_id.as_deref(), op, max_frame);
                std::process::exit(0);
            }
            "call.open" => {
                let ticket = env.body.get("ticket").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let cap = env.body.get("capability").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let input = env.body.get("input").cloned().unwrap_or(Value::Null);
                let cancel = Arc::new(AtomicBool::new(false));
                let mark_cancel = input.get("mark_cancel").and_then(|v| v.as_str()).map(|s| s.to_string());
                if let Some(cf) = input.get("count_file").and_then(|v| v.as_str()) {
                    append_line(cf, &ticket);
                }
                calls.lock().unwrap().insert(ticket.clone(), CallCtl { cancel: cancel.clone(), mark_cancel: mark_cancel.clone() });
                let w2 = writer.clone();
                let (sid, iid, gen) = (session_id.clone(), instance_id.clone(), generation);
                let rid = req_id.clone();
                let mid = format!("m-call-{}", ticket);
                let calls2 = calls.clone();
                std::thread::spawn(move || {
                    // Dorme de forma abortável se pedido.
                    let sleep_ms = input.get("sleep_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                    let mut slept = 0u64;
                    let mut aborted = false;
                    while slept < sleep_ms {
                        if cancel.load(Ordering::SeqCst) {
                            aborted = true;
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                        slept += 5;
                    }
                    calls2.lock().unwrap().remove(&ticket);
                    if aborted {
                        return; // host já abandonou a espera; sem resposta tardia.
                    }
                    if let Some(code) = input.get("fail").and_then(|v| v.as_str()) {
                        send(
                            &w2,
                            &json!({
                                "protocol": matrix_proto::PROTOCOL_ID,
                                "version": matrix_proto::PROTOCOL_VERSION,
                                "type": "call.result",
                                "message_id": mid,
                                "session_id": sid,
                                "instance_id": iid,
                                "generation": gen.to_string(),
                                "request_id": rid,
                                "body": {"ticket": ticket, "status": "error",
                                         "error": {"code": code, "message": format!("remote {}", code)}},
                            }),
                            max_frame,
                        );
                        return;
                    }
                    let _ = cap;
                    // Inundação opcional para a porta de crédito (M2.3).
                    if let Some(fl) = input.get("flood_stream") {
                        let fid = fl.get("id").and_then(|v| v.as_str()).unwrap_or("s1").to_string();
                        let chunk = fl.get("chunk").and_then(|v| v.as_u64()).unwrap_or(65536).min(1 << 20) as usize;
                        let count = fl.get("count").and_then(|v| v.as_u64()).unwrap_or(4).min(64);
                        let payload = "x".repeat(chunk);
                        for seq in 0..count {
                            if cancel.load(Ordering::SeqCst) {
                                break;
                            }
                            send(
                                &w2,
                                &json!({
                                    "protocol": matrix_proto::PROTOCOL_ID,
                                    "version": matrix_proto::PROTOCOL_VERSION,
                                    "type": "stream.data",
                                    "message_id": format!("m-{}-{}", ticket, seq),
                                    "session_id": sid,
                                    "instance_id": iid,
                                    "generation": gen.to_string(),
                                    "body": {"stream_id": fid, "seq": seq.to_string(), "payload": payload},
                                }),
                                max_frame,
                            );
                        }
                    }
                    send(
                        &w2,
                        &json!({
                            "protocol": matrix_proto::PROTOCOL_ID,
                            "version": matrix_proto::PROTOCOL_VERSION,
                            "type": "call.result",
                            "message_id": mid,
                            "session_id": sid,
                            "instance_id": iid,
                            "generation": gen.to_string(),
                            "request_id": rid,
                            "body": {"ticket": ticket, "status": "ok", "output": {"echo": input}},
                        }),
                        max_frame,
                    );
                });
            }
            "call.cancel" => {
                let ticket = env.body.get("ticket").and_then(|v| v.as_str()).unwrap_or("");
                // Marca uma única vez (quiesce + backstop do forward podem duplicar).
                if let Some(c) = calls.lock().unwrap().remove(ticket) {
                    c.cancel.store(true, Ordering::SeqCst);
                    if let Some(path) = c.mark_cancel.clone() {
                        append_line(&path, ticket);
                    }
                }
                send(
                    &writer,
                    &json!({
                        "protocol": matrix_proto::PROTOCOL_ID,
                        "version": matrix_proto::PROTOCOL_VERSION,
                        "type": "call.cancel.result",
                        "message_id": format!("m-cancel-{}", ticket),
                        "session_id": session_id,
                        "instance_id": instance_id,
                        "generation": generation.to_string(),
                        "request_id": req_id,
                        "body": {"ticket": ticket, "status": "cancelled"},
                    }),
                    max_frame,
                );
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn reply_lifecycle(
    writer: &Arc<Mutex<UnixStream>>,
    session_id: &str,
    instance_id: &str,
    generation: u64,
    in_reply_to: &str,
    request_id: Option<&str>,
    operation_id: Value,
    max_frame: usize,
) {
    let _ = in_reply_to;
    send(
        writer,
        &json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "lifecycle.result",
            "message_id": format!("m-lc-{}", operation_id.as_str().unwrap_or("op")),
            "session_id": session_id,
            "instance_id": instance_id,
            "generation": generation.to_string(),
            "request_id": request_id,
            "body": {"operation_id": operation_id, "status": "ok", "pending": []},
        }),
        max_frame,
    );
}

#[allow(dead_code)]
fn _use_types(_: Option<Envelope>) {}

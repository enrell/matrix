//! matrix-tui — REPL stub sobre o mesmo daemon/SDK.
//! ratatui entra aqui depois; o protocolo não muda (desktop usa o mesmo).

use matrix_sdk::{Agent, CannedModel, EchoTool, MatrixClient};
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::path::PathBuf;

fn home() -> PathBuf {
    if let Ok(h) = std::env::var("MATRIX_RT_HOME") {
        return PathBuf::from(h);
    }
    PathBuf::from(".")
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // `matrix-tui --agent "goal"` roda o loop ReAct de 5 passos (demo1).
    if args.get(1).map(|s| s.as_str()) == Some("--agent") {
        let goal = args.get(2).cloned().unwrap_or_else(|| "hello".into());
        let client = MatrixClient::new(home());
        let agent = Agent::new(CannedModel, EchoTool { client });
        println!("{}", agent.run(&goal));
        return;
    }
    // REPL: `invoke <cap> <json>` | `emit <topic> <json>` | `status` | `quit-tui`
    let client = MatrixClient::new(home());
    println!("matrix-tui — type `help`");
    let stdin = std::io::stdin();
    for line in stdin.lock().lines().map_while(Result::ok) {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if line == "quit-tui" || line == "exit" {
            break;
        }
        if line == "help" {
            println!("invoke <cap> <json> | emit <topic> <json> | status | --agent \"goal\"");
            continue;
        }
        let mut parts = line.splitn(3, ' ');
        let cmd = parts.next().unwrap_or("");
        match cmd {
            "invoke" => {
                let cap = parts.next().unwrap_or("");
                let input: Value = serde_json::from_str(parts.next().unwrap_or("{}")).unwrap_or(json!({}));
                match client.invoke(cap, input) {
                    Ok(v) => println!("{}", v),
                    Err(e) => println!("{{\"ok\":false,\"error\":{}}}", json!(e)),
                }
            }
            "emit" => {
                let topic = parts.next().unwrap_or("");
                let payload: Value =
                    serde_json::from_str(parts.next().unwrap_or("{}")).unwrap_or(json!({}));
                match client.emit(topic, payload) {
                    Ok(v) => println!("{}", v),
                    Err(e) => println!("{{\"ok\":false,\"error\":{}}}", json!(e)),
                }
            }
            "status" => match client.status() {
                Ok(v) => println!("{}", v),
                Err(e) => println!("{{\"ok\":false,\"error\":{}}}", json!(e)),
            },
            _ => println!("unknown cmd; type `help`"),
        }
        let _ = std::io::stdout().flush();
    }
}

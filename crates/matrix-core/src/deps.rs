//! Dependências e bindings (M1.2).
//!
//! Cobre C04/C05 e I04 no perfil confiável local:
//! - Manifest declara `requires` (interfaces obrigatórias) e `provides`
//!   (`capabilities` legado ou `provides` novo).
//! - Binding explícito (`provider`) ou provedor único; ambiguidade é erro,
//!   nunca "último registro vence".
//! - Versão major incompatível mantém `Waiting` com razão visível.
//! - Grafo obrigatório acíclico; ciclos rejeitados com caminho explicativo.
//!
//! Formatos aceitos para `requires` (cada item string ou objeto):
//! - `"workspace.fs@1"` — interface + major, provedor resolvido por unicidade.
//! - `"workspace.fs"` — interface sem major, casa qualquer versão.
//! - `{"interface":"workspace.fs@1"}` — igual ao string.
//! - `{"interface":"workspace.fs","major":1,"provider":"workspace"}` —
//!   binding explícito para o id lógico indicado.
//!
//! `provides` aceita os mesmos formatos de interface (sem `provider`).

use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    /// Interface pedida, normalizada (`base` ou `base@major`).
    pub interface: String,
    pub base: String,
    pub major: Option<u64>,
    /// Id lógico do provedor exigido, se binding explícito.
    pub provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub interface: String,
    pub provider_logical: String,
    pub provider_instance: u64,
    pub provider_generation: u64,
}

/// Separa `"ns.name@MAJOR"` em `(base, Some(major))`.
/// Sem `@`, retorna `(s, None)`. Major inválido → `None` (casa qualquer).
pub fn split_cap(s: &str) -> (String, Option<u64>) {
    let s = s.trim();
    match s.rfind('@') {
        Some(at) => {
            let (b, m) = s.split_at(at);
            match m[1..].parse::<u64>() {
                Ok(n) => (b.to_string(), Some(n)),
                Err(_) => (s.to_string(), None),
            }
        }
        None => (s.to_string(), None),
    }
}

pub fn norm_cap(base: &str, major: Option<u64>) -> String {
    match major {
        Some(m) => format!("{}@{}", base, m),
        None => base.to_string(),
    }
}

fn parse_require_value(v: &serde_json::Value) -> Result<Requirement, String> {
    match v {
        serde_json::Value::String(s) => {
            if s.trim().is_empty() {
                return Err("requires entry vazio".to_string());
            }
            let (base, major) = split_cap(s);
            if base.is_empty() {
                return Err(format!("requires inválido: {:?}", s));
            }
            Ok(Requirement {
                interface: norm_cap(&base, major),
                base,
                major,
                provider: None,
            })
        }
        serde_json::Value::Object(_) => {
            let iface = v
                .get("interface")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if iface.is_empty() {
                return Err("requires sem 'interface'".to_string());
            }
            let (mut base, mut major) = split_cap(&iface);
            if base.is_empty() {
                return Err(format!("requires inválido: {:?}", iface));
            }
            if let Some(m) = v.get("major").and_then(|x| x.as_u64()) {
                major = Some(m);
                base = split_cap(&iface).0;
                if base.is_empty() {
                    return Err(format!("requires inválido: {:?}", iface));
                }
            }
            let provider = v
                .get("provider")
                .and_then(|x| x.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            Ok(Requirement {
                interface: norm_cap(&base, major),
                base,
                major,
                provider,
            })
        }
        _ => Err("requires deve ser string ou objeto".to_string()),
    }
}

pub fn parse_requires(v: Option<&serde_json::Value>) -> Result<Vec<Requirement>, String> {
    let Some(v) = v else { return Ok(vec![]) };
    let arr = v.as_array().ok_or_else(|| "'requires' deve ser array".to_string())?;
    arr.iter().map(parse_require_value).collect()
}

/// Normaliza `provides` (novo) ou `capabilities` (legado) para caps
/// canônicos `"base@major"`. Aceita strings ou `{interface, major}`.
pub fn parse_provides(v: Option<&serde_json::Value>) -> Result<Vec<String>, String> {
    let Some(v) = v else { return Ok(vec![]) };
    let arr = v.as_array().ok_or_else(|| "'provides' deve ser array".to_string())?;
    let mut out = vec![];
    for item in arr {
        match item {
            serde_json::Value::String(s) => {
                if s.trim().is_empty() {
                    return Err("provides/capability vazio".to_string());
                }
                out.push(s.trim().to_string());
            }
            serde_json::Value::Object(_) => {
                let iface = item
                    .get("interface")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if iface.is_empty() {
                    return Err("provides sem 'interface'".to_string());
                }
                let (base, mut major) = split_cap(&iface);
                if let Some(m) = item.get("major").and_then(|x| x.as_u64()) {
                    major = Some(m);
                }
                if base.is_empty() {
                    return Err(format!("provides inválido: {:?}", iface));
                }
                out.push(norm_cap(&base, major));
            }
            _ => return Err("provides deve ter strings ou objetos".to_string()),
        }
    }
    Ok(out)
}

/// Um cap provido casa um requisito se base igual e major compatível
/// (requisito sem major casa qualquer versão).
pub fn cap_satisfies(provided_cap: &str, req: &Requirement) -> bool {
    let (base, major) = split_cap(provided_cap);
    if base != req.base {
        return false;
    }
    match (req.major, major) {
        (Some(want), Some(got)) => want == got,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    Missing { interface: String },
    Ambiguous { interface: String, providers: Vec<String> },
    VersionMismatch { interface: String, provider: String, detail: String },
    UnknownProvider { interface: String, provider: String },
    ProviderNotActive { interface: String, provider: String, state: String },
    Cycle { path: Vec<String> },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Missing { interface } => {
                write!(f, "dependency-unavailable: nenhuma definição provê {}", interface)
            }
            ResolveError::Ambiguous { interface, providers } => {
                write!(
                    f,
                    "ambiguous-provider: {} provido por [{}]; use binding explícito",
                    interface,
                    providers.join(", ")
                )
            }
            ResolveError::VersionMismatch { interface, provider, detail } => {
                write!(
                    f,
                    "dependency-unavailable: {} em {} incompatível ({})",
                    interface, provider, detail
                )
            }
            ResolveError::UnknownProvider { interface, provider } => {
                write!(
                    f,
                    "dependency-unavailable: provedor '{}' de {} desconhecido",
                    provider, interface
                )
            }
            ResolveError::ProviderNotActive { interface, provider, state } => {
                write!(
                    f,
                    "dependency-unavailable: provedor '{}' de {} está {}",
                    provider, interface, state
                )
            }
            ResolveError::Cycle { path } => {
                write!(f, "cycle: {}", path.join(" -> "))
            }
        }
    }
}

/// Grafo lógico consumidor → provedores explícitos ou candidatos.
/// `provides_of`: lógico → caps providos. `requires_of`: lógico → requisitos.
pub fn provider_candidates(
    req: &Requirement,
    provides_of: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut out: Vec<String> = provides_of
        .iter()
        .filter(|(_, caps)| caps.iter().any(|c| cap_satisfies(c, req)))
        .map(|(logical, _)| logical.clone())
        .collect();
    out.sort();
    out
}

/// Arestas do grafo para detecção de ciclo/ordem: para cada consumidor,
/// o conjunto de provedores que ele referencia (explícito, se houver;
/// senão todos os candidatos por versão).
pub fn graph_edges(
    requires_of: &HashMap<String, Vec<Requirement>>,
    provides_of: &HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();
    for (consumer, reqs) in requires_of {
        let mut deps: Vec<String> = vec![];
        for r in reqs {
            if let Some(p) = &r.provider {
                deps.push(p.clone());
            } else {
                deps.extend(provider_candidates(r, provides_of));
            }
        }
        deps.sort();
        deps.dedup();
        // Remove auto-aresta (será reportada como ciclo de 1 nó).
        edges.insert(consumer.clone(), deps);
    }
    // Garante nós provedores puros presentes.
    for logical in provides_of.keys() {
        edges.entry(logical.clone()).or_default();
    }
    edges
}

/// Encontra um ciclo alcançável; retorna o caminho `a -> b -> a`.
pub fn find_cycle(edges: &HashMap<String, Vec<String>>) -> Option<Vec<String>> {
    let mut color: HashMap<&str, u8> = HashMap::new();
    let mut stack: Vec<String> = vec![];

    fn dfs<'a>(
        node: &'a str,
        edges: &'a HashMap<String, Vec<String>>,
        color: &mut HashMap<&'a str, u8>,
        stack: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        color.insert(node, 1);
        stack.push(node.to_string());
        if let Some(nexts) = edges.get(node) {
            for nx in nexts {
                let c = color.get(nx.as_str()).copied().unwrap_or(0);
                if c == 1 {
                    let pos = stack.iter().position(|n| n == nx).unwrap_or(0);
                    let mut cyc: Vec<String> = stack[pos..].to_vec();
                    cyc.push(nx.clone());
                    return Some(cyc);
                }
                if c == 0 {
                    if let Some(cyc) = dfs(nx, edges, color, stack) {
                        return Some(cyc);
                    }
                }
            }
        }
        stack.pop();
        color.insert(node, 2);
        None
    }

    let mut nodes: Vec<&String> = edges.keys().collect();
    nodes.sort();
    for n in nodes {
        if color.get(n.as_str()).copied().unwrap_or(0) == 0 {
            if let Some(cyc) = dfs(n, edges, &mut color, &mut stack) {
                return Some(cyc);
            }
        }
    }
    None
}

/// Ordem topológica (provedores antes de consumidores). Retorna `None`
/// se houver ciclo. Inclui todos os nós do grafo.
pub fn topo_order(edges: &HashMap<String, Vec<String>>) -> Option<Vec<String>> {
    // Kahn sobre aresta consumidor -> provedor: grau = nº de deps.
    let mut indeg: HashMap<&str, usize> = HashMap::new();
    let mut rev: HashMap<&str, Vec<&str>> = HashMap::new();
    for (c, deps) in edges {
        indeg.entry(c.as_str()).or_insert(0);
        for d in deps {
            // consumidor depende de provedor: indeg(consumidor)+=1
            *indeg.entry(c.as_str()).or_insert(0) += 1;
            indeg.entry(d.as_str()).or_insert(0);
            rev.entry(d.as_str()).or_default().push(c.as_str());
        }
    }
    // Nós sem dependências primeiro (ordem alfabética para determinismo).
    let mut ready: Vec<&str> = indeg
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&n, _)| n)
        .collect();
    ready.sort_unstable();
    ready.reverse();
    let mut order: Vec<String> = vec![];
    while let Some(n) = ready.pop() {
        order.push(n.to_string());
        if let Some(deps) = rev.get(n) {
            let mut ds: Vec<&str> = deps.clone();
            ds.sort_unstable();
            for c in ds {
                if let Some(e) = indeg.get_mut(c) {
                    *e -= 1;
                    if *e == 0 {
                        ready.push(c);
                    }
                }
            }
        }
        // Mantém pop() devolvendo o menor nome (determinístico).
        ready.sort_unstable();
        ready.reverse();
    }
    if order.len() == indeg.len() {
        Some(order)
    } else {
        None
    }
}

/// Consumidores transitivos de `target` (quem depende dele, direta ou
/// indiretamente), via arestas revertidas. Não inclui o próprio alvo.
pub fn transitive_consumers(
    target: &str,
    edges: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut rev: HashMap<&str, Vec<&str>> = HashMap::new();
    for (c, deps) in edges {
        for d in deps {
            rev.entry(d.as_str()).or_default().push(c.as_str());
        }
    }
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack = vec![target.to_string()];
    seen.insert(target.to_string());
    let mut out: Vec<String> = vec![];
    while let Some(n) = stack.pop() {
        if let Some(cs) = rev.get(n.as_str()) {
            let mut sorted: Vec<&&str> = cs.iter().collect();
            sorted.sort();
            for c in sorted {
                if seen.insert(c.to_string()) {
                    out.push(c.to_string());
                    stack.push(c.to_string());
                }
            }
        }
    }
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provs(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn unique_provider_resolves() {
        let p = provs(&[("workspace", &["workspace.fs@1"])]);
        let r = parse_requires(Some(&serde_json::json!(["workspace.fs@1"]))).unwrap();
        assert_eq!(provider_candidates(&r[0], &p), vec!["workspace".to_string()]);
    }

    #[test]
    fn ambiguous_lists_providers() {
        let p = provs(&[("a", &["x.y@1"]), ("b", &["x.y@1"])]);
        let r = parse_requires(Some(&serde_json::json!(["x.y@1"]))).unwrap();
        assert_eq!(provider_candidates(&r[0], &p).len(), 2);
    }

    #[test]
    fn cycle_found_with_path() {
        let mut e = HashMap::new();
        e.insert("a".to_string(), vec!["b".to_string()]);
        e.insert("b".to_string(), vec!["a".to_string()]);
        let c = find_cycle(&e).unwrap();
        assert!(c.first() == c.last());
        assert!(c.contains(&"a".to_string()));
    }

    #[test]
    fn topo_providers_first() {
        let mut e = HashMap::new();
        e.insert("search".to_string(), vec!["workspace".to_string()]);
        e.insert("workspace".to_string(), vec![]);
        e.insert("echo".to_string(), vec![]);
        let o = topo_order(&e).unwrap();
        assert!(o.iter().position(|n| n == "workspace").unwrap()
            < o.iter().position(|n| n == "search").unwrap());
    }
}

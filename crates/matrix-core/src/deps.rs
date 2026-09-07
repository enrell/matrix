//! Dependencies and bindings (M1.2).
//!
//! Covers C04/C05 and I04 in the trusted local profile:
//! - Manifest declares `requires` (mandatory interfaces) and `provides`
//!   (legacy `capabilities` or new `provides`).
//! - Explicit binding (`provider`) or single provider; ambiguity is an error,
//!   never "last registration wins".
//! - Incompatible major version keeps `Waiting` with a visible reason.
//! - Mandatory graph is acyclic; cycles rejected with an explanatory path.
//!
//! Accepted formats for `requires` (each item a string or object):
//! - `"workspace.fs@1"` — interface + major, provider resolved by uniqueness.
//! - `"workspace.fs"` — interface without major, matches any version.
//! - `{"interface":"workspace.fs@1"}` — same as the string form.
//! - `{"interface":"workspace.fs","major":1,"provider":"workspace"}` —
//!   explicit binding to the given logical id.
//!
//! `provides` accepts the same interface formats (without `provider`).

use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    /// Requested interface, normalized (`base` or `base@major`).
    pub interface: String,
    pub base: String,
    pub major: Option<u64>,
    /// Required provider's logical id, if explicit binding.
    pub provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub interface: String,
    pub provider_logical: String,
    pub provider_instance: u64,
    pub provider_generation: u64,
}

/// Splits `"ns.name@MAJOR"` into `(base, Some(major))`.
/// Without `@`, returns `(s, None)`. Invalid major → `None` (matches any).
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
                return Err(format!("invalid requires: {:?}", s));
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
                return Err(format!("invalid requires: {:?}", iface));
            }
            if let Some(m) = v.get("major").and_then(|x| x.as_u64()) {
                major = Some(m);
                base = split_cap(&iface).0;
                if base.is_empty() {
                    return Err(format!("invalid requires: {:?}", iface));
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

/// Normalizes `provides` (new) or `capabilities` (legacy) into canonical
/// `"base@major"` caps. Accepts strings or `{interface, major}`.
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
                    return Err(format!("invalid provides: {:?}", iface));
                }
                out.push(norm_cap(&base, major));
            }
            _ => return Err("provides deve ter strings ou objetos".to_string()),
        }
    }
    Ok(out)
}

/// A provided cap satisfies a requirement if base matches and major is compatible
/// (a requirement without major matches any version).
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
                write!(f, "dependency-unavailable: no definition provides {}", interface)
            }
            ResolveError::Ambiguous { interface, providers } => {
                write!(
                    f,
                    "ambiguous-provider: {} provided by [{}]; use an explicit binding",
                    interface,
                    providers.join(", ")
                )
            }
            ResolveError::VersionMismatch { interface, provider, detail } => {
                write!(
                    f,
                    "dependency-unavailable: {} at {} is incompatible ({})",
                    interface, provider, detail
                )
            }
            ResolveError::UnknownProvider { interface, provider } => {
                write!(
                    f,
                    "dependency-unavailable: provider '{}' of {} is unknown",
                    provider, interface
                )
            }
            ResolveError::ProviderNotActive { interface, provider, state } => {
                write!(
                    f,
                    "dependency-unavailable: provider '{}' of {} is {}",
                    provider, interface, state
                )
            }
            ResolveError::Cycle { path } => {
                write!(f, "cycle: {}", path.join(" -> "))
            }
        }
    }
}

/// Logical consumer → explicit-or-candidate-providers graph.
/// `provides_of`: logical → provided caps. `requires_of`: logical → requirements.
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

/// Graph edges for cycle/order detection: for each consumer,
/// the set of providers it references (explicit if present;
/// otherwise all version candidates).
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
        // Drop self-edge (reported as a 1-node cycle).
        edges.insert(consumer.clone(), deps);
    }
    // Ensure pure-provider nodes are present.
    for logical in provides_of.keys() {
        edges.entry(logical.clone()).or_default();
    }
    edges
}

/// Finds a reachable cycle; returns the `a -> b -> a` path.
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

/// Topological order (providers before consumers). Returns `None`
/// on cycles. Includes every graph node.
pub fn topo_order(edges: &HashMap<String, Vec<String>>) -> Option<Vec<String>> {
    // Kahn over consumer -> provider edges: degree = dep count.
    let mut indeg: HashMap<&str, usize> = HashMap::new();
    let mut rev: HashMap<&str, Vec<&str>> = HashMap::new();
    for (c, deps) in edges {
        indeg.entry(c.as_str()).or_insert(0);
        for d in deps {
            // consumer depends on provider: indeg(consumer)+=1
            *indeg.entry(c.as_str()).or_insert(0) += 1;
            indeg.entry(d.as_str()).or_insert(0);
            rev.entry(d.as_str()).or_default().push(c.as_str());
        }
    }
    // Dependency-free nodes first (alphabetical order for determinism).
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
        // Keep pop() returning the smallest name (deterministic).
        ready.sort_unstable();
        ready.reverse();
    }
    if order.len() == indeg.len() {
        Some(order)
    } else {
        None
    }
}

/// Transitive consumers of `target` (direct or indirect dependents),
/// via reversed edges. Excludes the target itself.
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

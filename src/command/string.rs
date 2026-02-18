use std::time::Duration;

use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::SharedStore;
use crate::store::value::Value;

use super::bulk_to_string;

// ── SET with EX/PX ────────────────────────────────────────────────────────

pub(super) fn handle_set(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'set'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let val_bytes = match &args[1] {
        RespFrame::BulkString(Some(bytes)) => bytes.clone(),
        _ => return RespFrame::Error("ERR value must be bulk string".into()),
    };
    let value = Value::String(val_bytes.clone());

    // Parse optional flags: EX seconds | PX milliseconds
    let mut ttl: Option<Duration> = None;
    let mut aof_args: Vec<String> = vec!["SET".into(), key.clone()];
    aof_args.push(String::from_utf8_lossy(&val_bytes).into_owned());

    let mut i = 2;
    while i < args.len() {
        let flag = match bulk_to_string(&args[i]) {
            Some(s) => s.to_ascii_uppercase(),
            None => return RespFrame::Error("ERR syntax error".into()),
        };
        match flag.as_str() {
            "EX" => {
                i += 1;
                let secs = match args.get(i).and_then(bulk_to_string) {
                    Some(s) => match s.parse::<u64>() {
                        Ok(v) if v > 0 => v,
                        _ => return RespFrame::Error("ERR invalid expire time in 'set'".into()),
                    },
                    None => return RespFrame::Error("ERR syntax error".into()),
                };
                ttl = Some(Duration::from_secs(secs));
                aof_args.push("PX".into());
                aof_args.push((secs * 1000).to_string());
            }
            "PX" => {
                i += 1;
                let ms = match args.get(i).and_then(bulk_to_string) {
                    Some(s) => match s.parse::<u64>() {
                        Ok(v) if v > 0 => v,
                        _ => return RespFrame::Error("ERR invalid expire time in 'set'".into()),
                    },
                    None => return RespFrame::Error("ERR syntax error".into()),
                };
                ttl = Some(Duration::from_millis(ms));
                aof_args.push("PX".into());
                aof_args.push(ms.to_string());
            }
            _ => return RespFrame::Error("ERR syntax error".into()),
        }
        i += 1;
    }

    match store.write() {
        Ok(mut guard) => {
            match ttl {
                Some(dur) => guard.set_with_expiry(key, value, dur),
                None => guard.set(key, value),
            }
            if let Some(w) = aof {
                let refs: Vec<&str> = aof_args.iter().map(|s| s.as_str()).collect();
                w.append(&refs);
            }
            RespFrame::SimpleString("OK".into())
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

// ── GET ───────────────────────────────────────────────────────────────────

pub(super) fn handle_get(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'get'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    match store.write() {
        Ok(mut guard) => match guard.get(&key) {
            Some(Value::String(bytes)) => RespFrame::BulkString(Some(bytes)),
            Some(_) => RespFrame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            None => RespFrame::BulkString(None),
        },
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

// ── DEL ───────────────────────────────────────────────────────────────────

pub(super) fn handle_del(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.is_empty() {
        return RespFrame::Error("ERR wrong number of arguments for 'del'".into());
    }

    let mut keys = Vec::with_capacity(args.len());
    for arg in &args {
        match bulk_to_string(arg) {
            Some(k) => keys.push(k),
            None => return RespFrame::Error("ERR key must be bulk string".into()),
        }
    }

    match store.write() {
        Ok(mut guard) => {
            let removed = guard.del(&keys);
            if removed > 0 {
                if let Some(w) = aof {
                    let mut a = vec!["DEL"];
                    for k in &keys {
                        a.push(k);
                    }
                    w.append(&a);
                }
            }
            RespFrame::Integer(removed as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

// ── EXISTS ────────────────────────────────────────────────────────────────

pub(super) fn handle_exists(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.is_empty() {
        return RespFrame::Error("ERR wrong number of arguments for 'exists'".into());
    }

    let mut keys = Vec::with_capacity(args.len());
    for arg in &args {
        match bulk_to_string(arg) {
            Some(k) => keys.push(k),
            None => return RespFrame::Error("ERR key must be bulk string".into()),
        }
    }

    match store.write() {
        Ok(mut guard) => RespFrame::Integer(guard.exists(&keys) as i64),
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

// ── TTL / PTTL ────────────────────────────────────────────────────────────

pub(super) fn handle_ttl(args: Vec<RespFrame>, store: &SharedStore, millis: bool) -> RespFrame {
    if args.len() != 1 {
        let cmd = if millis { "pttl" } else { "ttl" };
        return RespFrame::Error(format!("ERR wrong number of arguments for '{cmd}'"));
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    match store.write() {
        Ok(mut guard) => {
            let ms = guard.ttl_millis(&key);
            if millis {
                RespFrame::Integer(ms)
            } else {
                match ms {
                    -2 | -1 => RespFrame::Integer(ms),
                    _ => RespFrame::Integer(ms / 1000),
                }
            }
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

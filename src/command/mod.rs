use std::time::Duration;

use bytes::Bytes;

use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::value::Value;
use crate::store::SharedStore;

fn bulk_to_string(frame: &RespFrame) -> Option<String> {
    match frame {
        RespFrame::BulkString(Some(bytes)) => String::from_utf8(bytes.to_vec()).ok(),
        _ => None,
    }
}

fn bulk_to_bytes(frame: &RespFrame) -> Option<Bytes> {
    match frame {
        RespFrame::BulkString(Some(bytes)) => Some(bytes.clone()),
        _ => None,
    }
}

/// Helper: collect all args as strings.
#[allow(dead_code)]
fn args_to_strings(args: &[RespFrame]) -> Option<Vec<String>> {
    args.iter().map(bulk_to_string).collect()
}

pub fn dispatch(frame: RespFrame, store: &SharedStore, aof: Option<&AofWriter>) -> RespFrame {
    match frame {
        RespFrame::Array(Some(items)) => handle_array(items, store, aof),
        _ => RespFrame::Error("ERR expected array".into()),
    }
}

fn handle_array(
    mut items: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if items.is_empty() {
        return RespFrame::Error("ERR empty command".into());
    }

    let command_frame = items.remove(0);
    let Some(cmd) = bulk_to_string(&command_frame) else {
        return RespFrame::Error("ERR command must be bulk string".into());
    };

    match cmd.to_ascii_uppercase().as_str() {
        "PING" => handle_ping(items),
        "ECHO" => handle_echo(items),
        "SET" => handle_set(items, store, aof),
        "GET" => handle_get(items, store),
        "DEL" => handle_del(items, store, aof),
        "EXISTS" => handle_exists(items, store),
        "TTL" => handle_ttl(items, store, false),
        "PTTL" => handle_ttl(items, store, true),
        "LPUSH" => handle_lpush(items, store, aof),
        "RPUSH" => handle_rpush(items, store, aof),
        "LPOP" => handle_lpop(items, store, aof),
        "RPOP" => handle_rpop(items, store, aof),
        "LRANGE" => handle_lrange(items, store),
        "SADD" => handle_sadd(items, store, aof),
        "SREM" => handle_srem(items, store, aof),
        "SMEMBERS" => handle_smembers(items, store),
        "HSET" => handle_hset(items, store, aof),
        "HGET" => handle_hget(items, store),
        "HGETALL" => handle_hgetall(items, store),
        _ => RespFrame::Error(format!("ERR unknown command '{cmd}'")),
    }
}

// ── Basic ─────────────────────────────────────────────────────────────────

fn handle_ping(args: Vec<RespFrame>) -> RespFrame {
    if args.is_empty() {
        RespFrame::SimpleString("PONG".into())
    } else if args.len() == 1 {
        match &args[0] {
            RespFrame::BulkString(Some(data)) => RespFrame::BulkString(Some(data.clone())),
            _ => RespFrame::Error("ERR PING expects bulk string".into()),
        }
    } else {
        RespFrame::Error("ERR too many arguments for PING".into())
    }
}

fn handle_echo(args: Vec<RespFrame>) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'echo'".into());
    }
    match &args[0] {
        RespFrame::BulkString(Some(data)) => RespFrame::BulkString(Some(data.clone())),
        _ => RespFrame::Error("ERR ECHO expects bulk string".into()),
    }
}

// ── SET with EX/PX ────────────────────────────────────────────────────────

fn handle_set(
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

fn handle_get(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
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

fn handle_del(
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

fn handle_exists(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
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

fn handle_ttl(args: Vec<RespFrame>, store: &SharedStore, millis: bool) -> RespFrame {
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

// ── List commands ─────────────────────────────────────────────────────────

fn handle_lpush(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'lpush'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut values = Vec::with_capacity(args.len() - 1);
    let mut val_strs = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match bulk_to_bytes(arg) {
            Some(b) => {
                val_strs.push(String::from_utf8_lossy(&b).into_owned());
                values.push(b);
            }
            None => return RespFrame::Error("ERR value must be bulk string".into()),
        }
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "list") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let len = guard.lpush(key.clone(), values);
            if let Some(w) = aof {
                let mut a: Vec<String> = vec!["LPUSH".into(), key];
                a.extend(val_strs);
                let refs: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
                w.append(&refs);
            }
            RespFrame::Integer(len as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_rpush(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'rpush'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut values = Vec::with_capacity(args.len() - 1);
    let mut val_strs = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match bulk_to_bytes(arg) {
            Some(b) => {
                val_strs.push(String::from_utf8_lossy(&b).into_owned());
                values.push(b);
            }
            None => return RespFrame::Error("ERR value must be bulk string".into()),
        }
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "list") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let len = guard.rpush(key.clone(), values);
            if let Some(w) = aof {
                let mut a: Vec<String> = vec!["RPUSH".into(), key];
                a.extend(val_strs);
                let refs: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
                w.append(&refs);
            }
            RespFrame::Integer(len as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_lpop(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.is_empty() || args.len() > 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'lpop'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let count: Option<usize> = if args.len() == 2 {
        match bulk_to_string(&args[1]).and_then(|s| s.parse::<usize>().ok()) {
            Some(n) => Some(n),
            None => return RespFrame::Error("ERR value is not an integer or out of range".into()),
        }
    } else {
        None
    };

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "list") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            match count {
                Some(n) => {
                    let mut items = Vec::with_capacity(n);
                    for _ in 0..n {
                        match guard.lpop(&key) {
                            Some(b) => items.push(RespFrame::BulkString(Some(b))),
                            None => break,
                        }
                    }
                    if !items.is_empty() {
                        if let Some(w) = aof {
                            for _ in 0..items.len() {
                                w.append(&["LPOP", &key]);
                            }
                        }
                    }
                    RespFrame::Array(Some(items))
                }
                None => match guard.lpop(&key) {
                    Some(b) => {
                        if let Some(w) = aof {
                            w.append(&["LPOP", &key]);
                        }
                        RespFrame::BulkString(Some(b))
                    }
                    None => RespFrame::BulkString(None),
                },
            }
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_rpop(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.is_empty() || args.len() > 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'rpop'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let count: Option<usize> = if args.len() == 2 {
        match bulk_to_string(&args[1]).and_then(|s| s.parse::<usize>().ok()) {
            Some(n) => Some(n),
            None => return RespFrame::Error("ERR value is not an integer or out of range".into()),
        }
    } else {
        None
    };

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "list") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            match count {
                Some(n) => {
                    let mut items = Vec::with_capacity(n);
                    for _ in 0..n {
                        match guard.rpop(&key) {
                            Some(b) => items.push(RespFrame::BulkString(Some(b))),
                            None => break,
                        }
                    }
                    if !items.is_empty() {
                        if let Some(w) = aof {
                            for _ in 0..items.len() {
                                w.append(&["RPOP", &key]);
                            }
                        }
                    }
                    RespFrame::Array(Some(items))
                }
                None => match guard.rpop(&key) {
                    Some(b) => {
                        if let Some(w) = aof {
                            w.append(&["RPOP", &key]);
                        }
                        RespFrame::BulkString(Some(b))
                    }
                    None => RespFrame::BulkString(None),
                },
            }
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_lrange(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 3 {
        return RespFrame::Error("ERR wrong number of arguments for 'lrange'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let start: i64 = match bulk_to_string(&args[1]).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespFrame::Error("ERR value is not an integer or out of range".into()),
    };

    let stop: i64 = match bulk_to_string(&args[2]).and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespFrame::Error("ERR value is not an integer or out of range".into()),
    };

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "list") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let items = guard.lrange(&key, start, stop);
            RespFrame::Array(Some(
                items
                    .into_iter()
                    .map(|b| RespFrame::BulkString(Some(b)))
                    .collect(),
            ))
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

// ── Set commands ──────────────────────────────────────────────────────────

fn handle_sadd(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'sadd'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut members = Vec::with_capacity(args.len() - 1);
    let mut mem_strs = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match bulk_to_bytes(arg) {
            Some(b) => {
                mem_strs.push(String::from_utf8_lossy(&b).into_owned());
                members.push(b);
            }
            None => return RespFrame::Error("ERR member must be bulk string".into()),
        }
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "set") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let added = guard.sadd(key.clone(), members);
            if added > 0 {
                if let Some(w) = aof {
                    let mut a: Vec<String> = vec!["SADD".into(), key];
                    a.extend(mem_strs);
                    let refs: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
                    w.append(&refs);
                }
            }
            RespFrame::Integer(added as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_srem(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'srem'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut members = Vec::with_capacity(args.len() - 1);
    let mut mem_strs = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match bulk_to_bytes(arg) {
            Some(b) => {
                mem_strs.push(String::from_utf8_lossy(&b).into_owned());
                members.push(b);
            }
            None => return RespFrame::Error("ERR member must be bulk string".into()),
        }
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "set") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let removed = guard.srem(&key, members);
            if removed > 0 {
                if let Some(w) = aof {
                    let mut a: Vec<String> = vec!["SREM".into(), key.to_string()];
                    a.extend(mem_strs);
                    let refs: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
                    w.append(&refs);
                }
            }
            RespFrame::Integer(removed as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_smembers(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'smembers'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "set") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let items = guard.smembers(&key);
            RespFrame::Array(Some(
                items
                    .into_iter()
                    .map(|b| RespFrame::BulkString(Some(b)))
                    .collect(),
            ))
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

// ── Hash commands ─────────────────────────────────────────────────────────

fn handle_hset(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return RespFrame::Error("ERR wrong number of arguments for 'hset'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut fields = Vec::with_capacity((args.len() - 1) / 2);
    let mut field_strs: Vec<String> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let field = match bulk_to_bytes(&args[i]) {
            Some(b) => b,
            None => return RespFrame::Error("ERR field must be bulk string".into()),
        };
        let value = match bulk_to_bytes(&args[i + 1]) {
            Some(b) => b,
            None => return RespFrame::Error("ERR value must be bulk string".into()),
        };
        field_strs.push(String::from_utf8_lossy(&field).into_owned());
        field_strs.push(String::from_utf8_lossy(&value).into_owned());
        fields.push((field, value));
        i += 2;
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "hash") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let added = guard.hset(key.clone(), fields);
            if let Some(w) = aof {
                let mut a: Vec<String> = vec!["HSET".into(), key];
                a.extend(field_strs);
                let refs: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
                w.append(&refs);
            }
            RespFrame::Integer(added as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_hget(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'hget'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let field = match bulk_to_bytes(&args[1]) {
        Some(b) => b,
        None => return RespFrame::Error("ERR field must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "hash") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            match guard.hget(&key, &field) {
                Some(b) => RespFrame::BulkString(Some(b)),
                None => RespFrame::BulkString(None),
            }
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

fn handle_hgetall(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'hgetall'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "hash") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let pairs = guard.hgetall(&key);
            let mut items = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                items.push(RespFrame::BulkString(Some(k)));
                items.push(RespFrame::BulkString(Some(v)));
            }
            RespFrame::Array(Some(items))
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

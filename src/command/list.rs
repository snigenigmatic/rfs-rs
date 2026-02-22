use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::SharedStore;

use super::{bulk_to_bytes, bulk_to_string};

pub(super) fn handle_lpush(
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

pub(super) fn handle_rpush(
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

pub(super) fn handle_lpop(
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
                    if !items.is_empty()
                        && let Some(w) = aof
                    {
                        for _ in 0..items.len() {
                            w.append(&["LPOP", &key]);
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

pub(super) fn handle_rpop(
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
                    if !items.is_empty()
                        && let Some(w) = aof
                    {
                        for _ in 0..items.len() {
                            w.append(&["RPOP", &key]);
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

pub(super) fn handle_lrange(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
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

pub(super) fn handle_llen(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'llen'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "list") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            RespFrame::Integer(guard.llen(&key) as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

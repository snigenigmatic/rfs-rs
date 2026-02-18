use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::SharedStore;

use super::{bulk_to_bytes, bulk_to_string};

pub(super) fn handle_sadd(
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

pub(super) fn handle_srem(
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
                    let mut a: Vec<String> = vec!["SREM".into(), key];
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

pub(super) fn handle_smembers(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
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

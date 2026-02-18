use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::SharedStore;

use super::{bulk_to_bytes, bulk_to_string};

pub(super) fn handle_hset(
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

pub(super) fn handle_hget(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
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

pub(super) fn handle_hgetall(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
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

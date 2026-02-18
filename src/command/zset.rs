use bytes::Bytes;

use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::SharedStore;

use super::{bulk_to_bytes, bulk_to_string};

pub(super) fn handle_zadd(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return RespFrame::Error("ERR wrong number of arguments for 'zadd'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut members = Vec::with_capacity((args.len() - 1) / 2);
    let mut mem_strs: Vec<String> = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let score = match bulk_to_string(&args[i]).and_then(|s| s.parse::<f64>().ok()) {
            Some(v) => v,
            None => return RespFrame::Error("ERR score is not a valid float".into()),
        };
        if !score.is_finite() {
            return RespFrame::Error("ERR score is not a valid float".into());
        }
        let member = match bulk_to_bytes(&args[i + 1]) {
            Some(b) => b,
            None => return RespFrame::Error("ERR member must be bulk string".into()),
        };
        mem_strs.push(score.to_string());
        mem_strs.push(String::from_utf8_lossy(&member).into_owned());
        members.push((member, score));
        i += 2;
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "zset") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let added = guard.zadd(key.clone(), members);
            if let Some(w) = aof {
                let mut a: Vec<String> = vec!["ZADD".into(), key];
                a.extend(mem_strs);
                let refs: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
                w.append(&refs);
            }
            RespFrame::Integer(added as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

pub(super) fn handle_zrange(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() < 3 {
        return RespFrame::Error("ERR wrong number of arguments for 'zrange'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let start = match bulk_to_string(&args[1]).and_then(|s| s.parse::<i64>().ok()) {
        Some(v) => v,
        None => return RespFrame::Error("ERR start is not an integer".into()),
    };

    let stop = match bulk_to_string(&args[2]).and_then(|s| s.parse::<i64>().ok()) {
        Some(v) => v,
        None => return RespFrame::Error("ERR stop is not an integer".into()),
    };

    let mut with_scores = false;
    if args.len() >= 4 {
        if let Some(opt) = bulk_to_string(&args[3]) {
            if opt.to_ascii_uppercase() == "WITHSCORES" {
                with_scores = true;
            }
        }
    }

    match store.read() {
        Ok(guard) => {
            let results = guard.zrange(&key, start, stop, with_scores);
            if results.is_empty() {
                return RespFrame::Array(Some(Vec::new()));
            }
            let mut frames = Vec::new();
            for (member, score_opt) in results {
                frames.push(RespFrame::BulkString(Some(member)));
                if let Some(score) = score_opt {
                    frames.push(RespFrame::BulkString(Some(Bytes::from(score.to_string()))));
                }
            }
            RespFrame::Array(Some(frames))
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

pub(super) fn handle_zscore(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'zscore'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let member = match bulk_to_bytes(&args[1]) {
        Some(b) => b,
        None => return RespFrame::Error("ERR member must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "zset") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            match guard.zscore(&key, &member) {
                Some(score) => RespFrame::BulkString(Some(Bytes::from(score.to_string()))),
                None => RespFrame::Null,
            }
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

pub(super) fn handle_zrank(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'zrank'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let member = match bulk_to_bytes(&args[1]) {
        Some(b) => b,
        None => return RespFrame::Error("ERR member must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "zset") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            match guard.zrank(&key, &member) {
                Some(rank) => RespFrame::Integer(rank as i64),
                None => RespFrame::Null,
            }
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

pub(super) fn handle_zcard(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'zcard'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "zset") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            RespFrame::Integer(guard.zcard(&key) as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}

pub(super) fn handle_zrem(
    args: Vec<RespFrame>,
    store: &SharedStore,
    aof: Option<&AofWriter>,
) -> RespFrame {
    if args.len() < 2 {
        return RespFrame::Error("ERR wrong number of arguments for 'zrem'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let mut members = Vec::with_capacity(args.len() - 1);
    let mut mem_strs: Vec<String> = Vec::new();
    for arg in &args[1..] {
        let member = match bulk_to_bytes(arg) {
            Some(b) => b,
            None => return RespFrame::Error("ERR member must be bulk string".into()),
        };
        mem_strs.push(String::from_utf8_lossy(&member).into_owned());
        members.push(member);
    }

    match store.write() {
        Ok(mut guard) => {
            if !guard.is_type(&key, "zset") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            let removed = guard.zrem(&key, members);
            if removed > 0 {
                if let Some(w) = aof {
                    let mut a: Vec<String> = vec!["ZREM".into(), key];
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

pub(super) fn handle_zcount(args: Vec<RespFrame>, store: &SharedStore) -> RespFrame {
    if args.len() != 3 {
        return RespFrame::Error("ERR wrong number of arguments for 'zcount'".into());
    }

    let key = match bulk_to_string(&args[0]) {
        Some(s) => s,
        None => return RespFrame::Error("ERR key must be bulk string".into()),
    };

    let min = match bulk_to_string(&args[1]).and_then(|s| s.parse::<f64>().ok()) {
        Some(v) => v,
        None => return RespFrame::Error("ERR min is not a valid float".into()),
    };

    let max = match bulk_to_string(&args[2]).and_then(|s| s.parse::<f64>().ok()) {
        Some(v) => v,
        None => return RespFrame::Error("ERR max is not a valid float".into()),
    };

    match store.read() {
        Ok(guard) => {
            if !guard.is_type(&key, "zset") {
                return RespFrame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
            RespFrame::Integer(guard.zcount(&key, min, max) as i64)
        }
        Err(_) => RespFrame::Error("ERR store lock poisoned".into()),
    }
}


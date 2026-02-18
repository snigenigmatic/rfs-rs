use crate::persistence::aof::AofWriter;
use crate::protocol::RespFrame;
use crate::store::SharedStore;

mod basic;
mod hash;
mod list;
mod set;
mod string;
mod zset;

use basic::{handle_echo, handle_ping};
use hash::{handle_hget, handle_hgetall, handle_hset};
use list::{handle_lpop, handle_lpush, handle_lrange, handle_rpop, handle_rpush};
use set::{handle_sadd, handle_smembers, handle_srem};
use string::{handle_del, handle_exists, handle_get, handle_set, handle_ttl};
use zset::{
    handle_zadd, handle_zcard, handle_zcount, handle_zrange, handle_zrank, handle_zrem,
    handle_zscore,
};

// ── Helpers (private here; accessible to all child modules via `super::`) ─

fn bulk_to_string(frame: &RespFrame) -> Option<String> {
    match frame {
        RespFrame::BulkString(Some(bytes)) => String::from_utf8(bytes.to_vec()).ok(),
        _ => None,
    }
}

fn bulk_to_bytes(frame: &RespFrame) -> Option<bytes::Bytes> {
    match frame {
        RespFrame::BulkString(Some(bytes)) => Some(bytes.clone()),
        _ => None,
    }
}

// ── Public entry point ────────────────────────────────────────────────────

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
        "ZADD" => handle_zadd(items, store, aof),
        "ZRANGE" => handle_zrange(items, store),
        "ZSCORE" => handle_zscore(items, store),
        "ZRANK" => handle_zrank(items, store),
        "ZCARD" => handle_zcard(items, store),
        "ZREM" => handle_zrem(items, store, aof),
        "ZCOUNT" => handle_zcount(items, store),
        _ => RespFrame::Error(format!("ERR unknown command '{cmd}'")),
    }
}

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::protocol::RespFrame;
use crate::protocol::encoder::encode_frame;
use crate::store::SharedStore;
use crate::store::value::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "always" => Self::Always,
            "no" | "never" => Self::No,
            _ => Self::EverySec,
        }
    }
}

/// Shared handle to the AOF writer.
#[derive(Clone)]
pub struct AofWriter {
    inner: Arc<Mutex<AofInner>>,
}

struct AofInner {
    writer: BufWriter<File>,
    policy: FsyncPolicy,
    last_fsync: Instant,
}

impl AofWriter {
    /// Open (or create) the AOF file at `path`.
    pub fn open(path: &Path, policy: FsyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(AofInner {
                writer: BufWriter::new(file),
                policy,
                last_fsync: Instant::now(),
            })),
        })
    }

    /// Append a command (as RESP array of bulk strings) to the AOF.
    pub fn append(&self, args: &[&str]) {
        let frame = RespFrame::Array(Some(
            args.iter()
                .map(|s| RespFrame::BulkString(Some(Bytes::copy_from_slice(s.as_bytes()))))
                .collect(),
        ));
        let mut buf = bytes::BytesMut::new();
        encode_frame(&frame, &mut buf);

        let mut inner = self.inner.lock().unwrap();
        if let Err(e) = inner.writer.write_all(&buf) {
            tracing::error!(error = %e, "aof write error");
            return;
        }

        match inner.policy {
            FsyncPolicy::Always => {
                let _ = inner.writer.flush();
                if let Ok(f) = inner.writer.get_ref().try_clone() {
                    let _ = f.sync_all();
                }
                inner.last_fsync = Instant::now();
            }
            FsyncPolicy::EverySec => {
                if inner.last_fsync.elapsed() >= Duration::from_secs(1) {
                    let _ = inner.writer.flush();
                    if let Ok(f) = inner.writer.get_ref().try_clone() {
                        let _ = f.sync_all();
                    }
                    inner.last_fsync = Instant::now();
                }
            }
            FsyncPolicy::No => {
                // Let the OS handle flushing.
            }
        }
    }
}

/// Replay the AOF to rebuild state on startup.
pub fn replay_aof(path: &Path, store: &SharedStore) -> io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut count: usize = 0;

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break; // EOF
        }
        let line = line.trim();
        if !line.starts_with('*') {
            continue; // skip malformed
        }

        let num_args: usize = match line[1..].parse() {
            Ok(n) => n,
            Err(_) => continue,
        };

        let mut args: Vec<String> = Vec::with_capacity(num_args);
        for _ in 0..num_args {
            // Read $<len>\r\n
            let mut len_line = String::new();
            if reader.read_line(&mut len_line)? == 0 {
                break;
            }
            let len_line = len_line.trim();
            if !len_line.starts_with('$') {
                break;
            }
            let len: usize = match len_line[1..].parse() {
                Ok(l) => l,
                Err(_) => break,
            };

            // Read <data>\r\n
            let mut data_line = String::new();
            if reader.read_line(&mut data_line)? == 0 {
                break;
            }
            let data = &data_line[..data_line.trim_end().len()];
            if data.len() != len {
                // Best effort
            }
            args.push(data.to_string());
        }

        if args.len() != num_args || args.is_empty() {
            continue;
        }

        replay_command(&args, store);
        count += 1;
    }

    Ok(count)
}

/// Execute a single command from the AOF replay against the store.
fn replay_command(args: &[String], store: &SharedStore) {
    let cmd = args[0].to_ascii_uppercase();
    let mut guard = store.write().unwrap();

    match cmd.as_str() {
        "SET" if args.len() >= 3 => {
            let key = args[1].clone();
            let val = Value::String(Bytes::copy_from_slice(args[2].as_bytes()));
            // TODO: handle EX/PX from AOF replay
            guard.set(key, val);
        }
        "DEL" if args.len() >= 2 => {
            let keys: Vec<String> = args[1..].to_vec();
            guard.del(&keys);
        }
        "LPUSH" if args.len() >= 3 => {
            let key = args[1].clone();
            let vals: Vec<Bytes> = args[2..]
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect();
            guard.lpush(key, vals);
        }
        "RPUSH" if args.len() >= 3 => {
            let key = args[1].clone();
            let vals: Vec<Bytes> = args[2..]
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect();
            guard.rpush(key, vals);
        }
        "LPOP" if args.len() >= 2 => {
            guard.lpop(&args[1]);
        }
        "RPOP" if args.len() >= 2 => {
            guard.rpop(&args[1]);
        }
        "SADD" if args.len() >= 3 => {
            let key = args[1].clone();
            let members: Vec<Bytes> = args[2..]
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect();
            guard.sadd(key, members);
        }
        "SREM" if args.len() >= 3 => {
            let members: Vec<Bytes> = args[2..]
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect();
            guard.srem(&args[1], members);
        }
        "HSET" if args.len() >= 4 && (args.len() - 2) % 2 == 0 => {
            let key = args[1].clone();
            let mut fields = Vec::new();
            let mut i = 2;
            while i + 1 < args.len() {
                fields.push((
                    Bytes::copy_from_slice(args[i].as_bytes()),
                    Bytes::copy_from_slice(args[i + 1].as_bytes()),
                ));
                i += 2;
            }
            guard.hset(key, fields);
        }
        "ZADD" if args.len() >= 4 && (args.len() - 2) % 2 == 0 => {
            let key = args[1].clone();
            let mut members = Vec::new();
            let mut i = 2;
            while i + 1 < args.len() {
                if let Ok(score) = args[i].parse::<f64>() {
                    let member = Bytes::copy_from_slice(args[i + 1].as_bytes());
                    members.push((member, score));
                }
                i += 2;
            }
            guard.zadd(key, members);
        }
        "ZREM" if args.len() >= 3 => {
            let members: Vec<Bytes> = args[2..]
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect();
            guard.zrem(&args[1], members);
        }
        _ => {
            tracing::debug!(cmd = %cmd, "skipping unknown AOF command during replay");
        }
    }
}

/// Rewrite the AOF: snapshot current state to a temp file, then atomically rename.
#[allow(dead_code)]
pub fn rewrite_aof(path: &Path, store: &SharedStore) -> io::Result<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let file = File::create(&tmp_path)?;
        let mut w = BufWriter::new(file);
        let guard = store.read().unwrap();

        // We need access to the internal data. Since Database fields are private,
        // we use the snapshot method we'll add.
        let snapshot = guard.snapshot_for_aof();
        for (key, value) in &snapshot {
            let mut buf = bytes::BytesMut::new();
            match value {
                Value::String(b) => {
                    let cmd = RespFrame::Array(Some(vec![
                        RespFrame::BulkString(Some(Bytes::from_static(b"SET"))),
                        RespFrame::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        RespFrame::BulkString(Some(b.clone())),
                    ]));
                    encode_frame(&cmd, &mut buf);
                }
                Value::List(deque) => {
                    if !deque.is_empty() {
                        let mut args = vec![
                            RespFrame::BulkString(Some(Bytes::from_static(b"RPUSH"))),
                            RespFrame::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        ];
                        for item in deque {
                            args.push(RespFrame::BulkString(Some(item.clone())));
                        }
                        encode_frame(&RespFrame::Array(Some(args)), &mut buf);
                    }
                }
                Value::Set(hs) => {
                    if !hs.is_empty() {
                        let mut args = vec![
                            RespFrame::BulkString(Some(Bytes::from_static(b"SADD"))),
                            RespFrame::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        ];
                        for item in hs {
                            args.push(RespFrame::BulkString(Some(item.clone())));
                        }
                        encode_frame(&RespFrame::Array(Some(args)), &mut buf);
                    }
                }
                Value::Hash(hm) => {
                    if !hm.is_empty() {
                        let mut args = vec![
                            RespFrame::BulkString(Some(Bytes::from_static(b"HSET"))),
                            RespFrame::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        ];
                        for (f, v) in hm {
                            args.push(RespFrame::BulkString(Some(f.clone())));
                            args.push(RespFrame::BulkString(Some(v.clone())));
                        }
                        encode_frame(&RespFrame::Array(Some(args)), &mut buf);
                    }
                }
                Value::ZSet(vec) => {
                    if !vec.is_empty() {
                        let mut args = vec![
                            RespFrame::BulkString(Some(Bytes::from_static(b"ZADD"))),
                            RespFrame::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        ];
                        for (m, s) in vec {
                            args.push(RespFrame::BulkString(Some(Bytes::copy_from_slice(
                                s.to_string().as_bytes(),
                            ))));
                            args.push(RespFrame::BulkString(Some(m.clone())));
                        }
                        encode_frame(&RespFrame::Array(Some(args)), &mut buf);
                    }
                }
            }
            w.write_all(&buf)?;
        }
        w.flush()?;
    }

    fs::rename(&tmp_path, path)?;
    Ok(())
}

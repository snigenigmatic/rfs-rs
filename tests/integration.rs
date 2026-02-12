use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

/// Spawn the server on a given port. Returns the child process handle.
fn spawn_server(port: u16) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_rfs-rs"))
        .args(["--bind", &format!("127.0.0.1:{port}")])
        .spawn()
        .expect("failed to start rfs-rs");

    // Give the server a moment to bind.
    std::thread::sleep(Duration::from_millis(500));
    child
}

/// Send raw RESP and read the response.
fn resp_roundtrip(stream: &mut TcpStream, request: &[u8]) -> String {
    stream.write_all(request).unwrap();
    stream.flush().unwrap();

    // Small sleep then read what's available.
    std::thread::sleep(Duration::from_millis(100));
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

/// Build a RESP array command from string args.
fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len());
    for arg in args {
        out.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    out.into_bytes()
}

#[test]
fn test_ping_pong() {
    let port = 16379;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["PING"]));
    assert_eq!(resp, "+PONG\r\n");

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_set_get_del() {
    let port = 16380;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["SET", "foo", "bar"]));
    assert_eq!(resp, "+OK\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["GET", "foo"]));
    assert_eq!(resp, "$3\r\nbar\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["EXISTS", "foo"]));
    assert_eq!(resp, ":1\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["DEL", "foo"]));
    assert_eq!(resp, ":1\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["GET", "foo"]));
    assert_eq!(resp, "$-1\r\n");

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_echo() {
    let port = 16381;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ECHO", "hello"]));
    assert_eq!(resp, "$5\r\nhello\r\n");

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_list_commands() {
    let port = 16382;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["RPUSH", "mylist", "a", "b", "c"]));
    assert_eq!(resp, ":3\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["LPUSH", "mylist", "z"]));
    assert_eq!(resp, ":4\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["LRANGE", "mylist", "0", "-1"]));
    assert_eq!(resp, "*4\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["LPOP", "mylist"]));
    assert_eq!(resp, "$1\r\nz\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["RPOP", "mylist"]));
    assert_eq!(resp, "$1\r\nc\r\n");

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_set_commands() {
    let port = 16383;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["SADD", "myset", "a", "b", "c"]));
    assert_eq!(resp, ":3\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["SADD", "myset", "a"]));
    assert_eq!(resp, ":0\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["SREM", "myset", "a"]));
    assert_eq!(resp, ":1\r\n");

    // SMEMBERS returns members in arbitrary order—just check it's an array of 2.
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["SMEMBERS", "myset"]));
    assert!(resp.starts_with("*2\r\n"));

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_hash_commands() {
    let port = 16384;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let resp = resp_roundtrip(
        &mut stream,
        &resp_cmd(&["HSET", "myhash", "f1", "v1", "f2", "v2"]),
    );
    assert_eq!(resp, ":2\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["HGET", "myhash", "f1"]));
    assert_eq!(resp, "$2\r\nv1\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["HGET", "myhash", "missing"]));
    assert_eq!(resp, "$-1\r\n");

    // HGETALL returns field-value pairs; 2 fields = 4 elements.
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["HGETALL", "myhash"]));
    assert!(resp.starts_with("*4\r\n"));

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_expiry() {
    let port = 16385;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // SET with PX 500 (500ms)
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["SET", "temp", "val", "PX", "500"]));
    assert_eq!(resp, "+OK\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["GET", "temp"]));
    assert_eq!(resp, "$3\r\nval\r\n");

    // Wait for expiry.
    std::thread::sleep(Duration::from_millis(700));

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["GET", "temp"]));
    assert_eq!(resp, "$-1\r\n");

    drop(stream);
    server.kill().ok();
}

#[test]
fn test_sorted_set_commands() {
    let port = 16386;
    let mut server = spawn_server(port);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    // ZADD: Add members with scores
    let resp = resp_roundtrip(
        &mut stream,
        &resp_cmd(&["ZADD", "myzset", "1.0", "one", "2.0", "two", "3.0", "three"]),
    );
    assert_eq!(resp, ":3\r\n");

    // ZADD: Update existing member's score
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZADD", "myzset", "2.5", "two"]));
    assert_eq!(resp, ":0\r\n");

    // ZCARD: Get cardinality
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZCARD", "myzset"]));
    assert_eq!(resp, ":3\r\n");

    // ZRANGE: Get range without scores
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZRANGE", "myzset", "0", "-1"]));
    assert_eq!(resp, "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n");

    // ZRANGE: Get range with scores
    let resp = resp_roundtrip(
        &mut stream,
        &resp_cmd(&["ZRANGE", "myzset", "0", "1", "WITHSCORES"]),
    );
    assert_eq!(
        resp,
        "*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$3\r\n2.5\r\n"
    );

    // ZSCORE: Get score of a member
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZSCORE", "myzset", "two"]));
    assert_eq!(resp, "$3\r\n2.5\r\n");

    // ZSCORE: Non-existent member
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZSCORE", "myzset", "nonexistent"]));
    assert_eq!(resp, "_\r\n");

    // ZRANK: Get rank of a member
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZRANK", "myzset", "one"]));
    assert_eq!(resp, ":0\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZRANK", "myzset", "two"]));
    assert_eq!(resp, ":1\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZRANK", "myzset", "three"]));
    assert_eq!(resp, ":2\r\n");

    // ZRANK: Non-existent member
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZRANK", "myzset", "nonexistent"]));
    assert_eq!(resp, "_\r\n");

    // ZCOUNT: Count members in score range
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZCOUNT", "myzset", "1.0", "2.5"]));
    assert_eq!(resp, ":2\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZCOUNT", "myzset", "0", "10"]));
    assert_eq!(resp, ":3\r\n");

    // ZREM: Remove members
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZREM", "myzset", "two"]));
    assert_eq!(resp, ":1\r\n");

    // Verify removal
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZCARD", "myzset"]));
    assert_eq!(resp, ":2\r\n");

    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZRANGE", "myzset", "0", "-1"]));
    assert_eq!(resp, "*2\r\n$3\r\none\r\n$5\r\nthree\r\n");

    // ZREM: Remove multiple members
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZREM", "myzset", "one", "three"]));
    assert_eq!(resp, ":2\r\n");

    // Verify all removed
    let resp = resp_roundtrip(&mut stream, &resp_cmd(&["ZCARD", "myzset"]));
    assert_eq!(resp, ":0\r\n");

    drop(stream);
    server.kill().ok();
}

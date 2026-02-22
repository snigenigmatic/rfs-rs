use std::io;

use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::encoder::encode_frame;

#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    Double(f64),
    Boolean(bool),
    Null,
    BulkString(Option<bytes::Bytes>),
    Array(Option<Vec<RespFrame>>),
    Map(Option<Vec<(RespFrame, RespFrame)>>),
    Set(Option<Vec<RespFrame>>),
    Push(Vec<RespFrame>),
}

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<RespError> for io::Error {
    fn from(value: RespError) -> Self {
        match value {
            RespError::Io(e) => e,
            RespError::Protocol(msg) => io::Error::new(io::ErrorKind::InvalidData, msg),
        }
    }
}

#[derive(Debug, Default)]
pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        match parse_frame(src) {
            Ok(Some((frame, used))) => {
                src.advance(used);
                Ok(Some(frame))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

impl Encoder<RespFrame> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_frame(&item, dst);
        Ok(())
    }
}

fn parse_frame(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        b'_' => parse_null(buf),
        b'#' => parse_boolean(buf),
        b',' => parse_double(buf),
        b'%' => parse_map(buf),
        b'~' => parse_set(buf),
        b'>' => parse_push(buf),
        _ => Err(RespError::Protocol("unknown prefix".into())),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn parse_simple_string(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let consumed = line_end + 2;
    let s = String::from_utf8(line.to_vec())
        .map_err(|e: std::string::FromUtf8Error| RespError::Protocol(e.to_string()))?;
    Ok(Some((RespFrame::SimpleString(s), consumed)))
}

fn parse_error(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let consumed = line_end + 2;
    let s = String::from_utf8(line.to_vec())
        .map_err(|e: std::string::FromUtf8Error| RespError::Protocol(e.to_string()))?;
    Ok(Some((RespFrame::Error(s), consumed)))
}

fn parse_integer(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let consumed = line_end + 2;
    let num: i64 = std::str::from_utf8(line)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| RespError::Protocol(e.to_string()))?;
    Ok(Some((RespFrame::Integer(num), consumed)))
}

fn parse_double(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let consumed = line_end + 2;
    let num: f64 = std::str::from_utf8(line)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseFloatError| RespError::Protocol(e.to_string()))?;
    Ok(Some((RespFrame::Double(num), consumed)))
}

fn parse_bulk_string(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let len_bytes = &buf[1..line_end];
    let len: isize = std::str::from_utf8(len_bytes)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| RespError::Protocol(e.to_string()))?;

    if len < -1 {
        return Err(RespError::Protocol("invalid bulk length".into()));
    }

    let consumed_head = line_end + 2;

    if len == -1 {
        return Ok(Some((RespFrame::BulkString(None), consumed_head)));
    }

    let len = len as usize;
    let needed = consumed_head + len + 2;
    if buf.len() < needed {
        return Ok(None);
    }

    if &buf[consumed_head + len..needed] != b"\r\n" {
        return Err(RespError::Protocol("bulk string missing CRLF".into()));
    }

    let data = buf[consumed_head..consumed_head + len].to_vec();
    let consumed = needed;
    Ok(Some((RespFrame::BulkString(Some(data.into())), consumed)))
}

fn parse_array(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let len_bytes = &buf[1..line_end];
    let len: isize = std::str::from_utf8(len_bytes)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| RespError::Protocol(e.to_string()))?;

    let mut consumed = line_end + 2;

    if len == -1 {
        return Ok(Some((RespFrame::Array(None), consumed)));
    }

    if len < 0 {
        return Err(RespError::Protocol("invalid array length".into()));
    }

    let len = len as usize;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        let slice = BytesMut::from(&buf[consumed..]);
        match parse_frame(&slice)? {
            Some((frame, used)) => {
                consumed += used;
                items.push(frame);
            }
            None => return Ok(None),
        }
    }

    Ok(Some((RespFrame::Array(Some(items)), consumed)))
}

fn parse_set(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let len_bytes = &buf[1..line_end];
    let len: isize = std::str::from_utf8(len_bytes)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| RespError::Protocol(e.to_string()))?;

    let mut consumed = line_end + 2;

    if len == -1 {
        return Ok(Some((RespFrame::Set(None), consumed)));
    }

    if len < 0 {
        return Err(RespError::Protocol("invalid set length".into()));
    }

    let len = len as usize;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        let slice = BytesMut::from(&buf[consumed..]);
        match parse_frame(&slice)? {
            Some((frame, used)) => {
                consumed += used;
                items.push(frame);
            }
            None => return Ok(None),
        }
    }

    Ok(Some((RespFrame::Set(Some(items)), consumed)))
}

fn parse_map(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let len_bytes = &buf[1..line_end];
    let len: isize = std::str::from_utf8(len_bytes)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| RespError::Protocol(e.to_string()))?;

    let mut consumed = line_end + 2;

    if len == -1 {
        return Ok(Some((RespFrame::Map(None), consumed)));
    }

    if len < 0 {
        return Err(RespError::Protocol("invalid map length".into()));
    }

    let len = len as usize;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        let slice_key = BytesMut::from(&buf[consumed..]);
        let Some((key, used_key)) = parse_frame(&slice_key)? else {
            return Ok(None);
        };
        consumed += used_key;

        let slice_val = BytesMut::from(&buf[consumed..]);
        let Some((val, used_val)) = parse_frame(&slice_val)? else {
            return Ok(None);
        };
        consumed += used_val;

        items.push((key, val));
    }

    Ok(Some((RespFrame::Map(Some(items)), consumed)))
}

fn parse_push(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    let Some(line_end) = find_crlf(&buf[1..]).map(|i| i + 1) else {
        return Ok(None);
    };
    let len_bytes = &buf[1..line_end];
    let len: isize = std::str::from_utf8(len_bytes)
        .map_err(|e: std::str::Utf8Error| RespError::Protocol(e.to_string()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| RespError::Protocol(e.to_string()))?;

    let mut consumed = line_end + 2;

    if len < 0 {
        return Err(RespError::Protocol("invalid push length".into()));
    }

    let len = len as usize;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        let slice = BytesMut::from(&buf[consumed..]);
        match parse_frame(&slice)? {
            Some((frame, used)) => {
                consumed += used;
                items.push(frame);
            }
            None => return Ok(None),
        }
    }

    Ok(Some((RespFrame::Push(items), consumed)))
}

fn parse_null(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    if buf.len() < 3 {
        return Ok(None);
    }
    if &buf[0..3] == b"_\r\n" {
        return Ok(Some((RespFrame::Null, 3)));
    }
    Err(RespError::Protocol("malformed null".into()))
}

fn parse_boolean(buf: &BytesMut) -> Result<Option<(RespFrame, usize)>, RespError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let val = match &buf[1..4] {
        b"t\r\n" => true,
        b"f\r\n" => false,
        _ => return Err(RespError::Protocol("invalid boolean".into())),
    };
    Ok(Some((RespFrame::Boolean(val), 4)))
}

#[allow(dead_code)]
pub fn frame_to_bytes(frame: &RespFrame) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_frame(frame, &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(buf: &[u8]) -> Vec<RespFrame> {
        let mut codec = RespCodec;
        let mut bytes = BytesMut::from(buf);
        let mut out = Vec::new();
        while let Some(frame) = codec.decode(&mut bytes).unwrap() {
            out.push(frame);
        }
        out
    }

    #[test]
    fn simple_string_roundtrip() {
        let frame = RespFrame::SimpleString("PONG".into());
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn error_roundtrip() {
        let frame = RespFrame::Error("ERR oops".into());
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn integer_roundtrip() {
        let frame = RespFrame::Integer(42);
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn bulk_string_roundtrip() {
        let frame = RespFrame::BulkString(Some(BytesMut::from("hello").freeze()));
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn null_bulk_string_roundtrip() {
        let frame = RespFrame::BulkString(None);
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn array_roundtrip() {
        let frame = RespFrame::Array(Some(vec![
            RespFrame::BulkString(Some(BytesMut::from("SET").freeze())),
            RespFrame::BulkString(Some(BytesMut::from("key").freeze())),
            RespFrame::BulkString(Some(BytesMut::from("val").freeze())),
        ]));

        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn null_array_roundtrip() {
        let frame = RespFrame::Array(None);
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn boolean_roundtrip() {
        for &b in &[true, false] {
            let frame = RespFrame::Boolean(b);
            let bytes = frame_to_bytes(&frame);
            assert_eq!(decode_all(&bytes), vec![frame.clone()]);
        }
    }

    #[test]
    fn double_roundtrip() {
        let frame = RespFrame::Double(std::f64::consts::PI);
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn null_roundtrip() {
        let frame = RespFrame::Null;
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn set_roundtrip() {
        let frame = RespFrame::Set(Some(vec![
            RespFrame::BulkString(Some(BytesMut::from("one").freeze())),
            RespFrame::BulkString(Some(BytesMut::from("two").freeze())),
        ]));
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn map_roundtrip() {
        let frame = RespFrame::Map(Some(vec![
            (
                RespFrame::BulkString(Some(BytesMut::from("k1").freeze())),
                RespFrame::BulkString(Some(BytesMut::from("v1").freeze())),
            ),
            (
                RespFrame::BulkString(Some(BytesMut::from("k2").freeze())),
                RespFrame::Integer(2),
            ),
        ]));
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }

    #[test]
    fn push_roundtrip() {
        let frame = RespFrame::Push(vec![
            RespFrame::SimpleString("pubsub".into()),
            RespFrame::BulkString(Some(BytesMut::from("chan").freeze())),
            RespFrame::BulkString(Some(BytesMut::from("msg").freeze())),
        ]);
        let bytes = frame_to_bytes(&frame);
        assert_eq!(decode_all(&bytes), vec![frame]);
    }
}

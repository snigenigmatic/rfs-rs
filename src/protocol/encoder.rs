use bytes::{BufMut, BytesMut};

use super::parser::RespFrame;

pub fn encode_frame(frame: &RespFrame, dst: &mut BytesMut) {
    match frame {
        RespFrame::SimpleString(s) => {
            dst.put_u8(b'+');
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Error(s) => {
            dst.put_u8(b'-');
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Integer(i) => {
            dst.put_u8(b':');
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Double(f) => {
            dst.put_u8(b',');
            dst.extend_from_slice(f.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Boolean(b) => {
            dst.put_u8(b'#');
            dst.put_u8(if *b { b't' } else { b'f' });
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Null => {
            dst.extend_from_slice(b"_\r\n");
        }
        RespFrame::BulkString(opt) => match opt {
            Some(bytes) => {
                dst.put_u8(b'$');
                dst.extend_from_slice(bytes.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(bytes);
                dst.extend_from_slice(b"\r\n");
            }
            None => dst.extend_from_slice(b"$-1\r\n"),
        },
        RespFrame::Array(opt) => match opt {
            Some(items) => {
                dst.put_u8(b'*');
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in items {
                    encode_frame(item, dst);
                }
            }
            None => dst.extend_from_slice(b"*-1\r\n"),
        },
        RespFrame::Set(opt) => match opt {
            Some(items) => {
                dst.put_u8(b'~');
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in items {
                    encode_frame(item, dst);
                }
            }
            None => dst.extend_from_slice(b"~-1\r\n"),
        },
        RespFrame::Map(opt) => match opt {
            Some(items) => {
                dst.put_u8(b'%');
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for (k, v) in items {
                    encode_frame(k, dst);
                    encode_frame(v, dst);
                }
            }
            None => dst.extend_from_slice(b"%-1\r\n"),
        },
        RespFrame::Push(items) => {
            dst.put_u8(b'>');
            dst.extend_from_slice(items.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, dst);
            }
        }
    }
}

use crate::protocol::RespFrame;

pub(super) fn handle_ping(args: Vec<RespFrame>) -> RespFrame {
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

pub(super) fn handle_echo(args: Vec<RespFrame>) -> RespFrame {
    if args.len() != 1 {
        return RespFrame::Error("ERR wrong number of arguments for 'echo'".into());
    }
    match &args[0] {
        RespFrame::BulkString(Some(data)) => RespFrame::BulkString(Some(data.clone())),
        _ => RespFrame::Error("ERR ECHO expects bulk string".into()),
    }
}

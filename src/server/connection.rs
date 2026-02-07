use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::command;
use crate::persistence::aof::AofWriter;
use crate::protocol::RespCodec;
use crate::store::SharedStore;

pub async fn handle_connection(
    stream: TcpStream,
    store: SharedStore,
    aof: Option<AofWriter>,
) -> std::io::Result<()> {
    let mut framed = Framed::new(stream, RespCodec::default());

    while let Some(frame) = framed.next().await {
        match frame {
            Ok(request) => {
                let response = command::dispatch(request, &store, aof.as_ref());
                // Ignore send errors (e.g., client closed) by breaking out.
                if let Err(err) = framed.send(response).await {
                    tracing::warn!(error = %err, "failed to send response");
                    break;
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "protocol error");
                break;
            }
        }
    }

    Ok(())
}

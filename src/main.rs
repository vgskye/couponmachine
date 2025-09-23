use std::{collections::HashMap, sync::LazyLock};

use eyre::eyre;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use rand::{rng, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use tracing::warn;

use crate::words::WORDS;

mod words;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Hello {
    realm: String,
    proto: u64,
    message: Vec<u8>,
}

const CURRENT_PROTO: u64 = 0;

#[derive(Serialize, Deserialize, Clone, Debug)]
enum FirstMessage {
    Create(CouponCreateRequest),
    Redeem(CouponRedeemStartRequest),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponCreateRequest {
    pake_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponCreateResponse {
    header: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponRedeemStartedNotice {
    pake_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponRedeemStartedResponse {
    data_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponRedeemFinishedNotice {
    data_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponRedeemStartRequest {
    header: String,
    pake_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponRedeemStartResponse {
    pake_message: Vec<u8>,
    data_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CouponRedeemFinishRequest {
    data_message: Vec<u8>,
}

struct CouponRedeemStartMessage {
    pake_message: Vec<u8>,
    socket: WebSocketStream<TcpStream>,
}

#[allow(clippy::type_complexity)]
static COUPONS: LazyLock<
    Mutex<HashMap<(String, String), oneshot::Sender<CouponRedeemStartMessage>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

struct CouponHandle {
    key: (String, String),
    receiver: Option<oneshot::Receiver<CouponRedeemStartMessage>>,
}

impl CouponHandle {
    fn new(realm: String) -> eyre::Result<(Self, &'static str)> {
        let (send, recv) = oneshot::channel();
        let mut coupons = COUPONS.lock();
        if coupons.len() >= WORDS.len() {
            return Err(eyre!("Too many open coupons!"));
        }
        let mut word = *WORDS.choose(&mut rng()).unwrap();
        while coupons.contains_key(&(realm.clone(), word.to_owned())) {
            word = *WORDS.choose(&mut rng()).unwrap();
        }
        coupons.insert((realm.clone(), word.to_owned()), send);
        Ok((
            Self {
                key: (realm, word.to_owned()),
                receiver: Some(recv),
            },
            word,
        ))
    }

    async fn get(mut self) -> CouponRedeemStartMessage {
        let msg = self.receiver.take().unwrap().await.unwrap();
        msg
    }
}

impl Drop for CouponHandle {
    fn drop(&mut self) {
        COUPONS.lock().remove(&self.key);
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt().init();
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(process_socket(socket));
    }
}

async fn process_socket(socket: TcpStream) {
    if let Err(e) = try_process_socket(socket).await {
        warn!("Error handling ws connection: {e:?}");
    }
}

async fn try_process_socket(socket: TcpStream) -> eyre::Result<()> {
    let mut ws = accept_async(socket).await?;
    let hello = ws
        .next()
        .await
        .ok_or(eyre!("Unexpectedly closed connection"))??;
    if !hello.is_binary() {
        return Err(eyre!("Unexpected message type, expected Binary"));
    }
    let hello: Hello = postcard::from_bytes(&hello.into_data())?;
    if hello.proto != CURRENT_PROTO {
        ws.send(Message::text("Unsupported protocol version!"))
            .await?;
        ws.close(None).await?;
        return Ok(());
    }
    let first: FirstMessage = postcard::from_bytes(&hello.message)?;
    match first {
        FirstMessage::Create(req) => {
            let (handle, header) = CouponHandle::new(hello.realm)?;
            ws.send(Message::binary(postcard::to_stdvec(
                &CouponCreateResponse {
                    header: header.to_string(),
                },
            )?))
            .await?;
            let mut msg = tokio::select! {
                msg = handle.get() => msg,
                msg = ws.next() => {
                    if let Some(msg) = msg {
                        msg?;
                        ws.send(Message::text("Unexpected message!")).await?;
                        ws.close(None).await?;
                    }
                    return Ok(())
                }
            };
            ws.send(Message::binary(postcard::to_stdvec(
                &CouponRedeemStartedNotice {
                    pake_message: msg.pake_message
                },
            )?))
            .await?;
            let resp = ws
                .next()
                .await
                .ok_or(eyre!("Unexpectedly closed connection"))??;
            if !resp.is_binary() {
                return Err(eyre!("Unexpected message type, expected Binary"));
            }
            let resp: CouponRedeemStartedResponse = postcard::from_bytes(&resp.into_data())?;
            msg.socket.send(Message::binary(postcard::to_stdvec(
                &CouponRedeemStartResponse {
                    pake_message: req.pake_message,
                    data_message: resp.data_message,
                },
            )?))
            .await?;
            let redeem_req = msg.socket
                .next()
                .await
                .ok_or(eyre!("Unexpectedly closed connection"))??;
            if !redeem_req.is_binary() {
                return Err(eyre!("Unexpected message type, expected Binary"));
            }
            let redeem_req: CouponRedeemFinishRequest = postcard::from_bytes(&redeem_req.into_data())?;
            ws.send(Message::binary(postcard::to_stdvec(
                &CouponRedeemFinishedNotice {
                    data_message: redeem_req.data_message
                },
            )?))
            .await?;
            ws.close(None).await?;
        }
        FirstMessage::Redeem(req) => {
            let Some(other) = COUPONS.lock().remove(&(hello.realm, req.header)) else {
                ws.send(Message::text("Invalid coupon!")).await?;
                ws.close(None).await?;
                return Ok(())
            };
            _ = other.send(CouponRedeemStartMessage { pake_message: req.pake_message, socket: ws });
        },
    }
    Ok(())
}

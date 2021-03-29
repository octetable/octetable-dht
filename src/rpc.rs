use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use std::io;


const FIND_NODE: u8 = 0;
const FIND_VALUE: u8 = 2;
const PING: u8 = 4;
const STORE: u8 = 8;

#[derive(Debug)]
pub enum RpcMessage {
    Ping,
    FindNode,
    FindValue,
    Store,
}

#[derive(Debug)]
pub enum CodecError {
    Io(io::Error),
    InvalidRpcMessage(u8)
}

impl From<io::Error> for CodecError {
    fn from(e: io::Error) -> Self {
        CodecError::Io(e)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct RpcCodec(());

impl RpcCodec {
    /// Creates a new `BytesCodec` for shipping around raw bytes.
    pub fn new() -> RpcCodec {
        RpcCodec(())
    }
}

impl Decoder for RpcCodec {
    type Item = RpcMessage;
    type Error = CodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.is_empty() {
            let len = buf.len();
            let mut data = buf.split_to(len);
            match data.get_u8() {
                FIND_NODE => Ok(Some(RpcMessage::FindNode)),
                FIND_VALUE => Ok(Some(RpcMessage::FindValue)),
                PING => Ok(Some(RpcMessage::Ping)),
                STORE => Ok(Some(RpcMessage::Store)),
                d => Err(CodecError::InvalidRpcMessage(d))
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<RpcMessage> for RpcCodec {
    type Error = io::Error;

    fn encode(&mut self, data: RpcMessage, buf: &mut BytesMut) -> Result<(), io::Error> {
        let d = match data {
            RpcMessage::FindNode => FIND_NODE,
            RpcMessage::FindValue => FIND_VALUE,
            RpcMessage::Ping => PING,
            RpcMessage::Store => STORE,
        };
        buf.put_u8(d);
        Ok(())
    }
}
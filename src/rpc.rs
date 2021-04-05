use std::io;

use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::{key::Key, routing::NodeInfo};
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Ping,
    FindNode(Key),
    FindValue(Key),
    Store(Key, Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Nodes(Vec<NodeInfo>),
    Pong,
    Value(Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Key, Request),
    Response(Key, Response),
}

impl Message {
    pub fn ping() -> Self {
        Message::Request(Key::rand(), Request::Ping)
    }

    pub fn find_node(node_id: Key) -> Self {
        Message::Request(Key::rand(), Request::FindNode(node_id))
    }

    pub fn find_value(value_id: Key) -> Self {
        Message::Request(Key::rand(), Request::FindValue(value_id))
    }

    pub fn store(value_id: Key, value: Vec<u8>) -> Self {
        Message::Request(Key::rand(), Request::Store(value_id, value))
    }

    pub fn pong(req_id: Key) -> Self {
        Message::Response(req_id, Response::Pong)
    }

    pub fn nodes(req_id: Key, node_infos: Vec<NodeInfo>) -> Self {
        Message::Response(req_id, Response::Nodes(node_infos))
    }

    pub fn value(req_id: Key, value: Vec<u8>) -> Self {
        Message::Response(req_id, Response::Value(value))
    }
}

pub struct MessageCodec {
    inner: LengthDelimitedCodec,
}

impl MessageCodec {
    pub fn new() -> Self {
        //TODO Customize HEAD frame to add checksum
        MessageCodec {
            inner: LengthDelimitedCodec::new(),
        }
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src) {
            Ok(Some(data)) => {
                //TODO validate checksum
                bincode::deserialize_from(data.reader())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
                    .map(Option::Some)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        bincode::serialize(&item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            //TODO compute checksum
            .and_then(|encoded| self.inner.encode(encoded.into(), dst))
    }
}

use std::io;
use std::net::SocketAddr;

use bytes::{Buf, BytesMut};
use futures::{select, stream::StreamExt, FutureExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::{
    codec::{Decoder, Encoder, LengthDelimitedCodec},
    udp::UdpFramed,
};

use crate::error::Error;
use crate::{key::Key, routing::NodeInfo};

#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectorRequest {
    Ping,
    FindNode(Key),
    FindValue(Key),
    Store(Key, Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectorResponse {
    Nodes(Vec<NodeInfo>),
    Pong,
    Value(Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectorMsg {
    Request(Key, ConnectorRequest),
    Response(Key, ConnectorResponse),
}

pub struct ConnectorCodec {
    inner: LengthDelimitedCodec,
}

impl ConnectorCodec {
    pub fn new() -> Self {
        //TODO Customize HEAD frame to add checksum
        ConnectorCodec {
            inner: LengthDelimitedCodec::new(),
        }
    }
}

impl Decoder for ConnectorCodec {
    type Item = ConnectorMsg;
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

impl Encoder<ConnectorMsg> for ConnectorCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ConnectorMsg, dst: &mut BytesMut) -> Result<(), Self::Error> {
        bincode::serialize(&item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            //TODO compute checksum
            .and_then(|encoded| self.inner.encode(encoded.into(), dst))
    }
}

pub type PingResult = Result<Key, ConnectorError>;

struct ConnectorHandle(UnboundedSender<(ConnectorMsg, SocketAddr)>);

impl ConnectorHandle {
    pub fn send_ping(&self, node_addr: SocketAddr) -> PingResult {
        let req_id = Key::rand();
        match self.0.send((
            ConnectorMsg::Request(req_id.clone(), ConnectorRequest::Ping),
            node_addr,
        )) {
            Ok(_) => Ok(req_id),
            Err(_) => Err(ConnectorError::Unreachable),
        }
    }
}

struct ConnectorRunner {
    tx_connector: UnboundedSender<(ConnectorMsg, SocketAddr)>,
    rx_connector: UnboundedReceiver<(ConnectorMsg, SocketAddr)>,
    addr: Option<SocketAddr>,
}

impl ConnectorRunner {
    pub fn new() -> Self {
        let (tx_connector, rx_connector) =
            tokio::sync::mpsc::unbounded_channel::<(ConnectorMsg, SocketAddr)>();
        Self {
            tx_connector,
            rx_connector,
            addr: None,
        }
    }

    pub fn connector_handle(&self) -> ConnectorHandle {
        ConnectorHandle(self.tx_connector.clone())
    }

    pub fn with_addr(mut self, addr: SocketAddr) -> Self {
        self.addr.replace(addr);
        self
    }

    pub fn addr(&mut self) -> Result<SocketAddr, Error> {
        self.addr
            .take()
            .ok_or(ConnectorError::MissingAddress.into())
    }

    fn run(mut self) -> Result<(), Error> {
        let addr = self.addr()?;

        // TODO find a better way to open socket
        let socket = futures::executor::block_on(UdpSocket::bind(&addr))
            .map_err(|e| ConnectorError::SocketOpen(addr, e))?;

        tokio::spawn(async move {
            let framed = UdpFramed::new(socket, ConnectorCodec::new());
            let (mut tx_socket, mut rx_socket) = framed.split();
            loop {
                select! {
                    res = rx_socket.next().fuse() => {
                        println!("Socket recv: {:?}", res);
                        // TODO send received message to Transporter or ...
                    },
                    res = self.rx_connector.recv().fuse() => {
                        println!("Socket send: {:?}", res);
                        match res {
                            Some((msg, addr)) => {
                                // TODO remove await in loop
                                if let Err(e) = tx_socket.send((msg, addr)).await {
                                    println!("Socket sender error: {:?}", e);
                                    break
                                }
                            }
                            _ => {
                                println!("Inbound channel closed!");
                                break
                            }
                        }
                    }
                }
            }
            println!("Connector closed!");
        });

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectorError {
    #[error("Couldn't contact Connector")]
    Unreachable,
    #[error("Missing address in ConnectorRunner")]
    MissingAddress,
    #[error("Couldn't open socket on address {0}: {1}")]
    SocketOpen(SocketAddr, io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::{net::UdpSocket, time};
    use tokio_stream::StreamExt;
    use tokio_util::udp::UdpFramed;

    async fn read_from_socket(socket: &mut UdpFramed<ConnectorCodec>) -> Result<(), ()> {
        let timeout = Duration::from_millis(200);
        while let Ok(Some(Ok((msg, addr)))) = time::timeout(timeout, socket.next()).await {
            match msg {
                ConnectorMsg::Request(req_id, ConnectorRequest::Store(key, data)) => {
                    println!(
                        "[socket] recv: {:?} Request(Key({:?}), Store(Key({:?}), {:?}))",
                        addr,
                        req_id,
                        key,
                        String::from_utf8(data)
                    );
                }
                msg => {
                    println!("[socket] recv: {:?} {:?}", addr, msg);
                }
            }
        }
        Ok(())
    }

    async fn send_to_connector(
        connector_handle: ConnectorHandle,
        addr: SocketAddr,
    ) -> Result<(), ()> {
        for _ in 0..4usize {
            println!(
                "Sending ping to connector: {:?}",
                connector_handle.send_ping(addr)
            )
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_send_to_connector() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let connector = ConnectorRunner::new().with_addr(addr);

        let handle = connector.connector_handle();

        connector.run().unwrap();

        let b = UdpSocket::bind(&addr).await.unwrap();
        let b_addr = b.local_addr().unwrap();

        let mut b = UdpFramed::new(b, ConnectorCodec::new());
        let read = read_from_socket(&mut b);

        let send = send_to_connector(handle, b_addr);
        // Start off by sending a ping from a to b, afterwards we just print out
        // what they send us and continually send pings

        // Run both futures simultaneously of `a` and `b` sending messages back and forth.
        match tokio::try_join!(read, send) {
            Err(e) => println!("an error occurred; error = {:?}", e),
            _ => println!("done!"),
        }
    }
}

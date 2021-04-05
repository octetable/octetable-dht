use std::net::SocketAddr;

use futures::{select, stream::StreamExt, FutureExt, SinkExt};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::udp::UdpFramed;

use crate::rpc::{Message, MessageCodec};

pub type ConnectorReceiver = UnboundedReceiver<(Message, SocketAddr)>;
pub type ConnectorSender = UnboundedSender<(Message, SocketAddr)>;

pub fn new_connector(addr: SocketAddr) -> (ConnectorSender, ConnectorReceiver) {
    let (outbound_tx, outbound_rx) =
        tokio::sync::mpsc::unbounded_channel::<(Message, SocketAddr)>();
    let (inbound_tx, mut inbound_rx) =
        tokio::sync::mpsc::unbounded_channel::<(Message, SocketAddr)>();
    tokio::spawn(async move {
        let socket = UdpSocket::bind(&addr).await.unwrap();
        let framed = UdpFramed::new(socket, MessageCodec::new());
        let (mut udp_tx, mut udp_rx) = framed.split();
        loop {
            select! {
                res = udp_rx.next().fuse() => {
                    println!("Socket recv: {:?}", res);
                    match res {
                        Some(Ok((msg, addr))) => {
                            if let Err(e) = outbound_tx.send((msg, addr)) {
                                println!("Socket sender error: {:?}", e);
                                break
                            }
                        }
                        _ => {
                            println!("Socket receiver closed!");
                            break
                        }
                    }
                },
                res = inbound_rx.recv().fuse() => {
                    println!("rx recv: {:?}", res);
                    match res {
                        Some((msg, addr)) => {
                            if let Err(e) = udp_tx.send((msg, addr)).await {
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

    (inbound_tx, outbound_rx)
}

#[cfg(test)]
mod tests {
    use crate::{key::Key, rpc::Request};

    use super::*;
    use futures::SinkExt;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::{net::UdpSocket, time};
    use tokio_stream::StreamExt;
    use tokio_util::udp::UdpFramed;

    fn select_msg() -> Message {
        match fastrand::u8(0..3) {
            0 => Message::store(Key::rand(), "Hello world!".into()),
            1 => Message::find_node(Key::rand()),
            2 => Message::find_value(Key::rand()),
            _ => Message::ping(),
        }
    }

    async fn read_from_socket(socket: &mut UdpFramed<MessageCodec>) -> Result<(), ()> {
        let timeout = Duration::from_millis(200);
        while let Ok(Some(Ok((msg, addr)))) = time::timeout(timeout, socket.next()).await {
            match msg {
                Message::Request(req_id, Request::Store(key, data)) => {
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

    async fn write_into_socket(
        socket: &mut UdpFramed<MessageCodec>,
        addr: SocketAddr,
    ) -> Result<(), ()> {
        for _ in 0..4usize {
            let msg = select_msg();
            println!("Sending {:?} into socket", msg);
            socket.send((msg, addr)).await.unwrap();
        }
        Ok(())
    }

    async fn send_to_connector(tx: ConnectorSender, addr: SocketAddr) -> Result<(), ()> {
        for _ in 0..4usize {
            let msg = select_msg();
            println!("Sending {:?} to connector", msg);
            tx.send((msg, addr)).unwrap();
        }
        Ok(())
    }

    async fn received_from_connector(mut rx: ConnectorReceiver) -> Result<(), ()> {
        let timeout = Duration::from_millis(200);
        while let Ok(Some((msg, addr))) = time::timeout(timeout, rx.recv()).await {
            match msg {
                Message::Request(req_id, Request::Store(key, data)) => {
                    println!(
                        "[connector] recv: {:?} Request(Key({:?}), Store(Key({:?}), {:?}))",
                        addr,
                        req_id,
                        key,
                        String::from_utf8(data)
                    );
                }
                msg => {
                    println!("[connector] recv: {:?} {:?}", addr, msg);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_send_to_connector() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let (a_tx, _) = new_connector(addr);

        let b = UdpSocket::bind(&addr).await.unwrap();
        let b_addr = b.local_addr().unwrap();

        let mut b = UdpFramed::new(b, MessageCodec::new());
        let read = read_from_socket(&mut b);

        let send = send_to_connector(a_tx, b_addr);
        // Start off by sending a ping from a to b, afterwards we just print out
        // what they send us and continually send pings

        // Run both futures simultaneously of `a` and `b` sending messages back and forth.
        match tokio::try_join!(read, send) {
            Err(e) => println!("an error occurred; error = {:?}", e),
            _ => println!("done!"),
        }
    }

    #[tokio::test]
    async fn test_read_from_connector() {
        let a_addr: SocketAddr = "127.0.0.1:5555".parse().unwrap();
        let (_a_tx, a_rx) = new_connector(a_addr);

        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let mut b = UdpFramed::new(b, MessageCodec::new());

        let write = write_into_socket(&mut b, a_addr);

        let recv = received_from_connector(a_rx);
        // Start off by sending a ping from a to b, afterwards we just print out
        // what they send us and continually send pings

        // Run both futures simultaneously of `a` and `b` sending messages back and forth.
        match tokio::try_join!(recv, write) {
            Err(e) => println!("an error occurred; error = {:?}", e),
            _ => println!("done!"),
        }
    }

    #[tokio::test]
    async fn all() {
        let c_addr: SocketAddr = "127.0.0.1:5556".parse().unwrap();

        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_addr = a.local_addr().unwrap();
        let mut a = UdpFramed::new(a, MessageCodec::new());
        let read_socket = read_from_socket(&mut a);

        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut b = UdpFramed::new(b, MessageCodec::new());

        let write_socket = write_into_socket(&mut b, c_addr);

        let (c_tx, c_rx) = new_connector(c_addr);
        let send_connector = send_to_connector(c_tx.clone(), a_addr); // c_tx is cloned to avoir closing read in the connector
        let recv_connector = received_from_connector(c_rx);
        match tokio::try_join!(read_socket, recv_connector, send_connector, write_socket) {
            Err(e) => println!("an error occurred; error = {:?}", e),
            _ => println!("done!"),
        }
    }
}

use tokio::{net::UdpSocket, sync::mpsc::{UnboundedReceiver, UnboundedSender}};
use tokio_util::{udp::UdpFramed};
use futures::{FutureExt, SinkExt, select, stream::StreamExt};
use std::net::SocketAddr;

use crate::{rpc::{RpcMessage, RpcCodec}};

pub type ConnectorReceiver = UnboundedReceiver<(RpcMessage, SocketAddr)>;
pub type ConnectorSender = UnboundedSender<(RpcMessage, SocketAddr)>;

pub fn new_connector(addr: SocketAddr) -> (ConnectorSender, ConnectorReceiver) {
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel::<(RpcMessage, SocketAddr)>();
    let (inbound_tx, mut inbound_rx) = tokio::sync::mpsc::unbounded_channel::<(RpcMessage, SocketAddr)>();
    tokio::spawn(async move {
        let socket = UdpSocket::bind(&addr).await.unwrap();
        let framed = UdpFramed::new(socket, RpcCodec::new());
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
    use super::*;
    use futures::SinkExt;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::{time, net::UdpSocket};
    use tokio_stream::StreamExt;
    use tokio_util::udp::UdpFramed;

    fn select_msg() -> RpcMessage {
        match fastrand::u8(0..3) {
            0 => RpcMessage::FindNode,
            1 => RpcMessage::FindValue,
            2 => RpcMessage::Ping,
            _ => RpcMessage::Store,
        }
    }

    async fn read_from_socket(socket: &mut UdpFramed<RpcCodec>) -> Result<(), ()> {
        let timeout = Duration::from_millis(200);
        while let Ok(Some(Ok((req, addr)))) = time::timeout(timeout, socket.next()).await {
            println!("[socket] recv: {:?} {:?}", addr, req);
        }
        Ok(())
    }

    async fn write_into_socket(socket: &mut UdpFramed<RpcCodec>, addr: SocketAddr) -> Result<(), ()> {
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
        while let Ok(Some((req, addr))) = time::timeout(timeout, rx.recv()).await {
            println!("[connector] recv: {:?} {:?}", addr, req);
        }
        Ok(())
    } 

    #[tokio::test]
    async fn test_send_to_connector() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let (a_tx, _) = new_connector(addr);
    
        let b = UdpSocket::bind(&addr).await.unwrap();
        let b_addr = b.local_addr().unwrap();

        
        let mut b = UdpFramed::new(b, RpcCodec::new());
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
        
        let mut b = UdpFramed::new(b, RpcCodec::new());
    
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
        let mut a = UdpFramed::new(a, RpcCodec::new());
        let read_socket = read_from_socket(&mut a);
        
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut b = UdpFramed::new(b, RpcCodec::new());
    
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
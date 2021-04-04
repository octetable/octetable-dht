use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time;

use crate::connector::ConnectorSender;
use crate::error::Result;
use crate::rpc::RpcMessage;

// TODO: should be defined in rpc module
pub type PingResult = std::result::Result<(), ()>;

#[derive(Clone)]
pub struct TransporterHandle(UnboundedSender<TranporterMsg>);

impl TransporterHandle {
    pub async fn ping(&self, addr: SocketAddr, timeout: Duration) -> Result<PingResult> {
        let (tx, rx) = oneshot::channel();

        self.0
            .send(TranporterMsg::SendPing((addr, tx)))
            .map_err(|_| TransporterError::Unreachable)?;

        match time::timeout(timeout, rx).await {
            Ok(Ok(res)) => Ok(res),

            // rx has been dropped
            Ok(Err(_)) => Err(TransporterError::DroppedRequest { addr }.into()),

            // request timed out
            Err(_) => {
                self.0
                    .send(TranporterMsg::CancelPing(addr))
                    .map_err(|_| TransporterError::Unreachable)?;
                Ok(PingResult::Err(()))
            }
        }
    }
}

// TODO: use snowflake id instead ? to handle edge cases of multiple pings to same addr
#[derive(Debug)]
enum TranporterMsg {
    SendPing((SocketAddr, oneshot::Sender<PingResult>)),
    HandlePing((SocketAddr, PingResult)),
    CancelPing(SocketAddr),
}

struct TransporterState {
    connector: ConnectorSender,
    pending: HashMap<SocketAddr, oneshot::Sender<PingResult>>,
}

impl TransporterState {
    fn send_ping(&mut self, addr: SocketAddr, tx: oneshot::Sender<PingResult>) {
        match self.connector.send((RpcMessage::Ping, addr)) {
            Ok(_) => {
                // TODO: check if addr is alreay pending
                self.pending.insert(addr, tx);
            }
            Err(e) => {
                // TODO: return a ConnectorError::Unreachable error instead ?
                log::error!("Couldn't contact connector: {}", e);
                tx.send(PingResult::Err(())).ok();
            }
        }
    }

    fn handle_ping(&mut self, addr: SocketAddr, res: PingResult) {
        if let Some(tx) = self.pending.remove(&addr) {
            tx.send(res).ok();
        } else {
            log::warn!("Tried to handle non existing ping request for: {}", addr);
        }
    }

    fn cancel_ping(&mut self, addr: SocketAddr) {
        if self.pending.remove(&addr).is_none() {
            log::warn!("Tried to cancel non existing ping request for: {}", addr);
        }
    }
}

pub struct TransporterRunner {
    tx_transporter: UnboundedSender<TranporterMsg>,
    rx_transporter: UnboundedReceiver<TranporterMsg>,
    connector: Option<ConnectorSender>,
}

impl TransporterRunner {
    pub fn new() -> Self {
        let (tx_transporter, rx_transporter) = mpsc::unbounded_channel();
        Self {
            tx_transporter,
            rx_transporter,
            connector: None,
        }
    }

    pub fn transporter_handle(&self) -> TransporterHandle {
        TransporterHandle(self.tx_transporter.clone())
    }

    pub fn with_connector(mut self, connector: ConnectorSender) -> Self {
        self.connector = Some(connector);
        self
    }

    fn connector(&mut self) -> Result<ConnectorSender> {
        self.connector
            .take()
            .ok_or(TransporterError::MissingConnector.into())
    }

    pub fn run(mut self) -> Result<()> {
        let connector = self.connector()?;

        tokio::spawn(async move {
            let mut state = TransporterState {
                connector,
                pending: HashMap::new(),
            };

            while let Some(msg) = self.rx_transporter.recv().await {
                use TranporterMsg::*;
                match msg {
                    SendPing((addr, tx)) => state.send_ping(addr, tx),
                    HandlePing((addr, res)) => state.handle_ping(addr, res),
                    CancelPing(addr) => state.cancel_ping(addr),
                }
            }
        });

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TransporterError {
    #[error("Missing ConnectorHandle in TransporterRunner")]
    MissingConnector,
    #[error("Couldn't contact Transporter")]
    Unreachable,
    #[error("Pending request for {addr} was dropped")]
    DroppedRequest { addr: SocketAddr },
}

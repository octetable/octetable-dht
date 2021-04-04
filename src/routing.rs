use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};

use crate::connector::ConnectorSender;
use crate::error::{Error, Result};
use crate::key::Key;
use crate::rpc::RpcMessage;
use crate::transporter::TransporterHandle;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct RouterHandle(UnboundedSender<RouterMsg>);

impl RouterHandle {
    pub fn add_node(&self, node_info: NodeInfo) -> Result<()> {
        todo!()
    }
}

enum RouterMsg {
    AddNode(NodeInfo),
}

struct RouterState {
    table: RoutingTable,
    transporter: TransporterHandle,
}

impl RouterState {
    async fn add_node(&mut self, node: NodeInfo) {
        // TODO: impl all the logic, this is just the critical async path

        let idx = self.table.me.dist_log2(node.key);
        let old_node = self.table.buckets[idx].nodes.front().unwrap();

        match self
            .transporter
            .ping(old_node.1.addr, Duration::from_secs(1)) // TODO: make timeout configurable
            .await
        {
            Ok(_) => (),
            Err(_) => (),
        }
    }
}

pub struct RouterRunner {
    tx_router: UnboundedSender<RouterMsg>,
    rx_router: UnboundedReceiver<RouterMsg>,
    me: Key,
    k: usize,
    transporter: Option<TransporterHandle>,
}

impl RouterRunner {
    pub fn new(me: Key) -> Self {
        let (tx_router, rx_router) = mpsc::unbounded_channel();
        Self {
            tx_router,
            rx_router,
            me,
            k: 20,
            transporter: None,
        }
    }

    pub fn router_handle(&self) -> RouterHandle {
        RouterHandle(self.tx_router.clone())
    }

    pub fn bucket_size(mut self, k: usize) -> Self {
        self.k = k;
        self
    }

    pub fn with_transporter(mut self, transporter: TransporterHandle) -> Self {
        self.transporter = Some(transporter);
        self
    }

    fn transporter(&mut self) -> Result<TransporterHandle> {
        self.transporter
            .take()
            .ok_or(RouterError::MissingTransporter.into())
    }

    pub fn run(mut self) -> Result<()> {
        let transporter = self.transporter()?;

        tokio::spawn(async move {
            let state = Arc::new(Mutex::new(RouterState {
                table: RoutingTable::new(self.me, self.k),
                transporter,
            }));

            while let Some(msg) = self.rx_router.recv().await {
                use RouterMsg::*;
                match msg {
                    AddNode(node) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            // TODO: this holds the lock up to timeout duration ...
                            // TODO: so it's probably better to split this method
                            state.lock().await.add_node(node).await;
                        });
                    }
                }
            }
        });

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RouterError {
    #[error("Missing TranporterHandle in RouterRunner")]
    MissingTransporter,
}

////////////////////////////////////////////////////////////////////////////////////////

pub struct NodeInfo {
    key: Key,
    addr: SocketAddr,
}

pub struct KBucket {
    k: usize,
    nodes: VecDeque<(Instant, NodeInfo)>,
}

impl KBucket {
    fn new(k: usize) -> Self {
        let nodes = VecDeque::with_capacity(k);
        Self { k, nodes }
    }
}

pub struct RoutingTable {
    me: Key,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(me: Key, k: usize) -> Self {
        let buckets = (0..160).map(|_| KBucket::new(k)).collect();
        Self { me, buckets }
    }
}

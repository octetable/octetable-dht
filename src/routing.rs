use std::collections::VecDeque;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::key::Key;

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeInfo {
    key: Key,
    address: SocketAddr,
}

pub struct KBucket {
    nodes: VecDeque<NodeInfo>,
}

impl KBucket {
    fn new() -> Self {
        let nodes = VecDeque::new();
        Self { nodes }
    }

    pub fn put(&mut self, node_info: NodeInfo) {
        self.nodes.push_back(node_info)
    }
}

pub struct RoutingTable {
    me: Key,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(me: Key) -> Self {
        let buckets = (0..160).map(|_| KBucket::new()).collect();
        Self { me, buckets }
    }

    pub fn put(&mut self, node_info: NodeInfo) {
        let dist = self.me.dist_log2(node_info.key);
        self.buckets[dist].put(node_info);
    }
}

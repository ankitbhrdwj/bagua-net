use crate::interface::{
    BaguaNetError, NCCLNetProperties, Net, SocketHandle, SocketListenCommID, SocketRecvCommID,
    SocketRequestID, SocketSendCommID,
};
use crate::utils;
use nix::sys::socket::{InetAddr, SockAddr};
use socket2::{Domain, Socket, Type};
use std::collections::HashMap;

use anyhow::Context;
use dpdk::eal::*;
use std::sync::Arc;
use std::sync::Mutex;

const NCCL_PTR_HOST: i32 = 1;
const NCCL_PTR_CUDA: i32 = 2;

#[derive(Debug)]
pub struct RequestState {
    pub nsubtasks: usize,
    pub completed_subtasks: usize,
    pub nbytes_transferred: usize,
    pub err: Option<BaguaNetError>,
}

#[derive(Clone)]
pub struct SocketSendComm {
    pub tcp_sender: Arc<std::thread::JoinHandle<()>>,
    pub msg_sender: flume::Sender<(&'static [u8], Arc<Mutex<RequestState>>)>,
}

#[derive(Clone)]
pub struct SocketRecvComm {
    pub tcp_sender: Arc<std::thread::JoinHandle<()>>,
    pub msg_sender: flume::Sender<(&'static mut [u8], Arc<Mutex<RequestState>>)>,
}

struct SocketSendRequest {}
struct SocketRecvRequest {}

pub enum SocketRequest {
    SendRequest(SocketSendRequest),
    RecvRequest(SocketRecvRequest),
}

// There is no actual listener in DPDK, so we just use a placeholder
struct SocketListenComm {}

pub struct BaguaNet {
    devices: Vec<utils::NCCLSocketDev>,
    pub listen_comm_next_id: usize,
    pub listen_comm_map: HashMap<SocketListenCommID, SocketListenComm>,
    pub send_comm_next_id: usize,
    pub send_comm_map: HashMap<SocketSendCommID, SocketSendComm>,
    pub recv_comm_next_id: usize,
    pub recv_comm_map: HashMap<SocketRecvCommID, SocketRecvComm>,
    pub socket_request_next_id: usize,
    pub socket_request_map: HashMap<SocketRequestID, SocketRequest>,
    // some fields omitted
}

fn network_args(pci_path: String) -> Vec<String> {
    let args = vec![
        "nccl_plugin".to_string(),
        String::from("-a"),
        pci_path,
        String::from("-l"),
        "0".to_string(),
        String::from("--main-lcore"),
        "0".to_string(),
    ];
    args
}

impl BaguaNet {
    const DEFAULT_SOCKET_MAX_COMMS: i32 = 65536;
    const DEFAULT_LISTEN_BACKLOG: i32 = 16384;

    pub fn new() -> Result<BaguaNet, BaguaNetError> {
        let devices = utils::find_interfaces();
        if devices.is_empty() {
            return Err(BaguaNetError::InnerError(
                "No available network devices found".to_string(),
            ));
        }
        let eal = Eal::new(&mut network_args(devices[0].pci_path.clone()))
            .context("initializing EAL")
            .unwrap();
        let lcores = eal.lcores();

        Ok(BaguaNet {
            devices: utils::find_interfaces(),
            listen_comm_next_id: 0,
            listen_comm_map: Default::default(),
            send_comm_next_id: 0,
            send_comm_map: Default::default(),
            recv_comm_next_id: 0,
            recv_comm_map: Default::default(),
            socket_request_next_id: 0,
            socket_request_map: Default::default(),
        })
    }
}

impl Net for BaguaNet {
    fn devices(&self) -> Result<usize, BaguaNetError> {
        Ok(self.devices.len())
    }

    fn get_properties(&self, dev_id: usize) -> Result<NCCLNetProperties, BaguaNetError> {
        let socket_dev = &self.devices[dev_id];

        let p = NCCLNetProperties {
            name: socket_dev.interface_name.clone(),
            pci_path: socket_dev.pci_path.clone(),
            guid: dev_id as u64,
            ptr_support: NCCL_PTR_HOST,
            speed: utils::get_net_if_speed(&socket_dev.interface_name),
            port: 0,
            max_comms: BaguaNet::DEFAULT_SOCKET_MAX_COMMS,
        };
        Ok(p)
    }

    fn listen(
        &mut self,
        dev_id: usize,
    ) -> Result<(SocketHandle, SocketListenCommID), BaguaNetError> {
        let socket_dev = &self.devices[dev_id];
        let addr = match socket_dev.addr {
            SockAddr::Inet(inet_addr) => inet_addr,
            others => {
                return Err(BaguaNetError::InnerError(format!(
                    "Got invalid socket address, which is {:?}",
                    others
                )))
            }
        };

        let id = self.listen_comm_next_id;
        self.listen_comm_next_id += 1;
        let socket_addr = InetAddr::new(addr.ip(), id as u16);
        let socket = match Socket::new(
            match addr {
                InetAddr::V4(_) => Domain::IPV4,
                InetAddr::V6(_) => Domain::IPV6,
            },
            Type::STREAM,
            None,
        ) {
            Ok(sock) => sock,
            Err(err) => return Err(BaguaNetError::IOError(format!("{:?}", err))),
        };
        socket.bind(&addr.to_std().into()).unwrap();
        socket.listen(BaguaNet::DEFAULT_LISTEN_BACKLOG).unwrap();

        let socket_handle = SocketHandle {
            addr: SockAddr::new_inet(socket_addr),
        };
        Ok((socket_handle, id))
    }

    fn connect(
        &mut self,
        _dev_id: usize,
        socket_handle: SocketHandle,
    ) -> Result<SocketSendCommID, BaguaNetError> {
        // Send a request to other side to establish a connection
        let id = self.send_comm_next_id;
        self.send_comm_next_id += 1;
        Ok(id)
    }

    fn accept(
        &mut self,
        listen_comm_id: SocketListenCommID,
    ) -> Result<SocketRecvCommID, BaguaNetError> {
        // Accept request from other side to establish a connection
        let id = self.recv_comm_next_id;
        self.recv_comm_next_id += 1;
        Ok(id)
    }

    fn isend(
        &mut self,
        send_comm_id: SocketSendCommID,
        data: &'static [u8],
    ) -> Result<SocketRequestID, BaguaNetError> {
        let request_id = self.socket_request_next_id;
        self.socket_request_next_id += 1;
        Ok(request_id)
    }

    fn irecv(
        &mut self,
        recv_comm_id: SocketRecvCommID,
        data: &'static mut [u8],
    ) -> Result<SocketRequestID, BaguaNetError> {
        let request_id = self.socket_request_next_id;
        self.socket_request_next_id += 1;
        Ok(request_id)
    }

    fn test(&mut self, request_id: SocketRequestID) -> Result<(bool, usize), BaguaNetError> {
        Ok((true, 131072))
    }
    fn close_send(&mut self, send_comm_id: SocketSendCommID) -> Result<(), BaguaNetError> {
        Ok(())
    }

    fn close_recv(&mut self, recv_comm_id: SocketRecvCommID) -> Result<(), BaguaNetError> {
        Ok(())
    }

    fn close_listen(&mut self, listen_comm_id: SocketListenCommID) -> Result<(), BaguaNetError> {
        Ok(())
    }
}

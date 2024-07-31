use crate::interface::{
    BaguaNetError, NCCLNetProperties, Net, SocketHandle, SocketListenCommID, SocketRecvCommID,
    SocketRequestID, SocketSendCommID,
};
use crate::utils;
use nix::sys::socket::{InetAddr, SockAddr};
use std::collections::HashMap;

use std::sync::Arc;
use std::sync::Mutex;

use super::ffi::*;
use super::tcp::*;

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

    // libtcp specific fields
    workers: Vec<Box<tpa_worker>>,
}

impl BaguaNet {
    const DEFAULT_SOCKET_MAX_COMMS: i32 = 65536;
    const DEFAULT_LISTEN_BACKLOG: i32 = 16384;
    const NR_WORKERS: i32 = 1;

    pub fn new() -> Result<BaguaNet, BaguaNetError> {
        let devices = utils::find_interfaces();
        if devices.is_empty() {
            return Err(BaguaNetError::InnerError(
                "No available network devices found".to_string(),
            ));
        }
        libtcp_config(&devices[0]).expect("network_config failed");
        tcp_init(BaguaNet::NR_WORKERS).expect("tcp_init failed");
        let workers = (0..BaguaNet::NR_WORKERS)
            .map(|_| tcp_worker_init())
            .collect();

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
            workers,
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
        let local_ip = addr.ip();
        let port = self.listen_comm_next_id as u16;
        println!("Listening on {}:{}", local_ip, port);
        //let fd = tcp_listen_on(local_ip.to_string(), port, None).expect("tcp_listen_on failed");
        let socket_handle = SocketHandle {
            addr: SockAddr::new_inet(InetAddr::new(local_ip, port)),
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
        let fd = match socket_handle.addr {
            SockAddr::Inet(inet_addr) => {
                let ip = inet_addr.ip();
                let port = inet_addr.port();
                println!("Connecting to {}:{}", ip, port);
                //tcp_connect_to(ip.to_string(), port, None).expect("tcp_connect_to failed")
                0
            }
            _ => {
                return Err(BaguaNetError::InnerError(
                    "Got invalid socket address".to_string(),
                ))
            }
        };
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

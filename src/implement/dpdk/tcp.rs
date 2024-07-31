//TODO: implement the following functions
//
// pub fn tpa_accept_burst(sid: ::std::os::raw::c_int, burst: *mut ::std::os::raw::c_int) -> ::std::os::raw::c_int;
// pub fn tpa_sock_info_get(sid: ::std::os::raw::c_int, info: *mut tpa_sock_info) -> ::std::os::raw::c_int;
// pub fn tpa_close(sid: ::std::os::raw::c_int) -> ::std::os::raw::c_int;
// pub fn tpa_zreadv(sid: ::std::os::raw::c_int, iov: *mut ::std::os::raw::c_void, iovcnt: ::std::os::raw::c_int) -> ::std::os::raw::c_int;
// pub fn tpa_zwritev(sid: ::std::os::raw::c_int, iov: *mut ::std::os::raw::c_void, iovcnt: ::std::os::raw::c_int) -> ::std::os::raw::c_int;
//
// pub fn tpa_memsegs_get() -> *mut tpa_memseg;
// pub fn tpa_extmem_register
// pub fn tpa_extmem_unregister

use crate::utils;
use anyhow::Error;
use libtcp::ffi::tpa_event;
use nix::sys::socket::{IpAddr, Ipv4Addr, SockAddr};
use std::fs::File;
use std::io::Write;
use std::mem::MaybeUninit;

// Don't change this function without checking
// https://github.com/bytedance/libtpa/blob/main/doc/user_guide.rst#config-options
pub fn libtcp_config(device: &utils::NCCLSocketDev) -> Result<(), Error> {
    let mut file = File::create("tpa.cfg").unwrap();
    let ip = match device.addr {
        SockAddr::Inet(inet_addr) => match inet_addr.ip() {
            IpAddr::V4(ipv4) => ipv4,
            _ => {
                return Err(Error::msg("Invalid socket address"));
            }
        },
        _ => return Err(Error::msg("Invalid socket address")),
    };
    let octets = ip.octets();
    let gw = IpAddr::V4(Ipv4Addr::new(octets[0], octets[1], octets[2], 0));

    file.write_all(b"net { ").unwrap();
    file.write_all(format!("name = {}; ", device.interface_name).as_bytes())
        .unwrap();
    file.write_all(format!("ip = {}; ", ip).as_bytes()).unwrap();
    file.write_all(format!("gw = {}; ", gw).as_bytes()).unwrap();
    file.write_all(b"mask = 255.255.255.0; }\n").unwrap();
    file.write_all(format!("dpdk {{ pci = {}; }}\n", device.pci_path).as_bytes())
        .unwrap();
    file.flush().unwrap();
    Ok(())
}

// TCP worker related functions
pub fn tcp_init(nr_worker: i32) -> Result<(), std::io::Error> {
    match unsafe { libtcp::ffi::tpa_init(nr_worker) } {
        0 => Ok(()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_init failed",
        )),
    }
}

pub fn tcp_worker_init() -> Box<libtcp::ffi::tpa_worker> {
    match unsafe { libtcp::ffi::tpa_worker_init() } {
        ptr if ptr.is_null() => panic!("tpa_worker_init failed"),
        ptr => unsafe { Box::from_raw(ptr) },
    }
}

pub fn tcp_worker_run(worker: &mut Box<libtcp::ffi::tpa_worker>) {
    unsafe { libtcp::ffi::tpa_worker_run(worker.as_mut()) }
}

// TCP connection related functions
pub fn tcp_connect_to(
    server: String,
    port: u16,
    opts: Option<libtcp::ffi::tpa_sock_opts>,
) -> Result<i32, std::io::Error> {
    let server = std::ffi::CString::new(server).unwrap();
    match unsafe {
        libtcp::ffi::tpa_connect_to(
            server.as_bytes().as_ptr() as *const i8,
            port,
            opts.map_or(std::ptr::null(), |opts| &opts),
        )
    } {
        sid if sid < 0 => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_connect_to failed",
        )),
        sid => Ok(sid),
    }
}

pub fn tcp_listen_on(
    local_ip: String,
    port: u16,
    opts: Option<libtcp::ffi::tpa_sock_opts>,
) -> Result<i32, std::io::Error> {
    let local = std::ffi::CString::new(local_ip).unwrap();
    let ptr = if local.is_empty() {
        std::ptr::null()
    } else {
        local.as_bytes().as_ptr() as *const i8
    };

    match unsafe {
        libtcp::ffi::tpa_listen_on(ptr, port, opts.map_or(std::ptr::null(), |opts| &opts))
    } {
        sid if sid < 0 => Err(std::io::Error::new(
            std::io::Error::from_raw_os_error(sid).kind(),
            "tpa_listen_on failed",
        )),
        sid => Ok(sid),
    }
}

pub fn tcp_write(sid: i32, buf: &[u8]) -> Result<isize, std::io::Error> {
    match unsafe { libtcp::ffi::tpa_write(sid, buf.as_ptr() as *const std::ffi::c_void, buf.len()) }
    {
        ret if ret < 0 => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_write failed",
        )),
        ret => Ok(ret),
    }
}

// Event related functions
pub fn tcp_event_register(sid: i32, events: u32) -> Result<(), std::io::Error> {
    let mut uninit = MaybeUninit::<tpa_event>::uninit();
    let event = uninit.as_mut_ptr();
    let mut event = unsafe {
        (*event).events = events;
        uninit.assume_init()
    };

    match unsafe {
        libtcp::ffi::tpa_event_ctrl(sid, libtcp::ffi::TPA_EVENT_CTRL_ADD as i32, &mut event)
    } {
        0 => Ok(()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_event_ctrl failed",
        )),
    }
}

pub fn tcp_event_update(sid: i32, events: u32) -> Result<(), std::io::Error> {
    let mut uninit = MaybeUninit::<tpa_event>::uninit();
    let event = uninit.as_mut_ptr();
    let mut event = unsafe {
        (*event).events = events;
        uninit.assume_init()
    };
    match unsafe {
        libtcp::ffi::tpa_event_ctrl(sid, libtcp::ffi::TPA_EVENT_CTRL_MOD as i32, &mut event)
    } {
        0 => Ok(()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_event_ctrl failed",
        )),
    }
}

pub fn tcp_event_poll(
    worker: &mut Box<libtcp::ffi::tpa_worker>,
    events: &mut [tpa_event],
    maxevents: i32,
) -> i32 {
    assert!(events.len() as i32 >= maxevents);
    unsafe { libtcp::ffi::tpa_event_poll(worker.as_mut(), events.as_mut_ptr(), maxevents) }
}

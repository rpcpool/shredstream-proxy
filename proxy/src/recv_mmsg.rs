use std::{
    cmp, io, mem::{self, MaybeUninit, zeroed}, net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket}, num, os::fd::AsRawFd, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, time::{Duration, Instant}
};

use bytes::{Buf, BufMut};
use itertools::izip;
use libc::{AF_INET, AF_INET6, MSG_DONTWAIT, iovec, mmsghdr, msghdr, sockaddr_storage};
use log::error;
use mio::{Poll, Token, Waker};
use socket2::socklen_t;
use solana_perf::packet::PACKETS_PER_BATCH;
use solana_sdk::packet::{Meta, PACKET_DATA_SIZE};
use solana_streamer::{streamer::StreamerReceiveStats};

use crate::{mem::{FrameBuf, FrameBufMut, FrameDesc, Rx, Tx}, prom::{inc_packets_received, inc_routing_drop, inc_routing_send, observe_recv_packet_count}};

pub trait PacketRoutingStrategy: Clone {
    fn route_packet(&self, packet: &TritonPacket, num_dest: usize) -> Option<usize>;
}

#[inline]
fn hash_pair(x: u64, y: u32) -> u64 {
    let mut h = x ^ ((y as u64) << 32);
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h
}

#[derive(Debug, Clone)]
pub struct FECSetRoutingStrategy;

impl PacketRoutingStrategy for FECSetRoutingStrategy {
    fn route_packet(&self, packet: &TritonPacket, num_dest: usize) -> Option<usize> {
        if num_dest == 0 {
            return None;
        }
        let shred_buf = packet.buffer.chunk();
        let slot = solana_ledger::shred::wire::get_slot(shred_buf)?;
        let fec = shred_buf.get(79..79 + 4)?;
        let fec_bytes: [u8; 4] = fec.try_into().ok()?;
        let fec_set_index = u32::from_le_bytes(fec_bytes);
        let hash = hash_pair(slot, fec_set_index);
        let dest = (hash as usize) % num_dest;
        Some(dest)
    }
}

pub fn recv_loop<R>(
    sk_vec: Vec<UdpSocket>,
    exit: &AtomicBool,
    stats: &StreamerReceiveStats,
    fill_rx: &mut Rx<FrameDesc>,
    packet_tx_vec: &[Tx<TritonPacket>],
    wake_slot: Arc<Mutex<Option<Arc<Waker>>>>,
    router: R,
) -> std::io::Result<()>
where
    R: PacketRoutingStrategy,
{
    assert!(packet_tx_vec.len() > 0, "packet_tx_vec must have at least one destination");
    let mut packet_batch = Vec::with_capacity(PACKETS_PER_BATCH);
    let mut frame_bufmut_vec = Vec::with_capacity(PACKETS_PER_BATCH);
    let mut next_stats_report = Instant::now() + Duration::from_secs(1);
    let mut router_dest_dist = vec![0usize; packet_tx_vec.len()];
    let mut router_dest_label_vec = Vec::with_capacity(packet_tx_vec.len());
    for idx in 0..packet_tx_vec.len() {
        router_dest_label_vec.push(idx.to_string());
    }
    let mut poll = Poll::new()?;
    const WAKE_TOKEN: Token = Token(usize::MAX);
    let mut events = mio::Events::with_capacity(sk_vec.len() + 1);
    let wake_handle = Arc::new(Waker::new(poll.registry(), WAKE_TOKEN)?);
    *wake_slot.lock().expect("recv wake slot lock poisoned") = Some(Arc::clone(&wake_handle));
    let mut empty_fill_backoff = 0u32;

    let mut mio_sockets: Vec<mio::net::UdpSocket> = sk_vec
        .iter()
        .map(|sk| mio::net::UdpSocket::from_std(sk.try_clone().unwrap()))
        .collect();
    // Initial registration of sockets
    for (i, socket) in mio_sockets.iter_mut().enumerate() {
        poll.registry().register(
            socket,
            mio::Token(i),
            mio::Interest::READABLE,
        )?;
    }

    while !exit.load(Ordering::Relaxed) {

        // Events are always cleared before receiving new ones
        let result = poll.poll(&mut events, None);

        match result {
            Ok(_) => { }
            Err(e) => {
                if e.kind() != io::ErrorKind::TimedOut {
                    return Err(e);
                }
            }
        }

        if next_stats_report.elapsed() > Duration::ZERO {
            next_stats_report = Instant::now() + Duration::from_secs(1);
            log::trace!(
                "recv_loop: packets_count={}, packet_batches_count={}, full_packet_batches_count={}",
                stats.packets_count.load(Ordering::Relaxed),
                stats.packet_batches_count.load(Ordering::Relaxed),
                stats.full_packet_batches_count.load(Ordering::Relaxed),
            );
        }
        // Check for exit signal, even if socket is busy
        // (for instance the leader transaction socket)
        if exit.load(Ordering::Relaxed) {
            return Ok(());
        }
        // We can't use a for-loop here because we need to be able to drain the readiness of each socket.
        // Since each recv_from is bounded by a PACKETS_PER_BATCH, we may need to call recv_from multiple times per socket
        // until we get a WouldBlock error.
        let mut ev_iter = events.iter();
        let Some(mut ev) = ev_iter.next() else {
            continue;
        };
        'drain_readiness_loop: while !exit.load(Ordering::Relaxed) {
            if ev.token() == WAKE_TOKEN {
                if exit.load(Ordering::Relaxed) {
                    return Ok(());
                }
                match ev_iter.next() {
                    Some(next_ev) => {
                        ev = next_ev;
                        continue 'drain_readiness_loop;
                    }
                    None => break 'drain_readiness_loop,
                }
            }

            let sk_idx = ev.token().0;
            let recv_sk = &sk_vec[sk_idx];

            // Refill the frame buffers as much as we can,
            'fill_bufmut: while frame_bufmut_vec.len() < PACKETS_PER_BATCH {
                let maybe_frame_buf = fill_rx.try_recv();
                match maybe_frame_buf {
                    Some(frame_desc) => {
                        let frame_bufmut = frame_desc.as_mut_buf();
                        frame_bufmut_vec.push(frame_bufmut);
                        empty_fill_backoff = 0;
                    }
                    None => {
                        if frame_bufmut_vec.is_empty() {
                            if exit.load(Ordering::Relaxed) {
                                break 'fill_bufmut;
                            }
                            empty_fill_backoff = empty_fill_backoff.saturating_add(1);
                            if empty_fill_backoff <= 128 {
                                std::hint::spin_loop();
                            } else {
                                std::thread::yield_now();
                            }
                            break 'fill_bufmut;
                        } else {
                            break 'fill_bufmut;
                        }
                    }
                }
            }

            if frame_bufmut_vec.is_empty() {
                // No available frame buffers to receive into, wait a bit
                log::debug!("recv_loop: no available frame buffers to receive into");
                continue 'drain_readiness_loop;
            }
            
            let result = recv_from(&mut frame_bufmut_vec, recv_sk, &mut packet_batch, &exit);

            match result {
                Ok(len) => {
                    if len > 0 {
                        // observe_recv_interval(recv_interval.as_micros() as f64);
                        inc_packets_received(len as u64);
                        observe_recv_packet_count(len as f64);
                        let StreamerReceiveStats {
                            packets_count,
                            packet_batches_count,
                            full_packet_batches_count,
                            ..
                        } = stats;

                        packets_count.fetch_add(len, Ordering::Relaxed);
                        packet_batches_count.fetch_add(1, Ordering::Relaxed);
                        if len == PACKETS_PER_BATCH {
                            full_packet_batches_count.fetch_add(1, Ordering::Relaxed);
                        }
                        packet_batch
                            .iter_mut()
                            .for_each(|p| p.meta_mut().set_from_staked_node(false));

                        'packet_drain: for packet in packet_batch.drain(..) {
                            let dest_idx = match router.route_packet(&packet, packet_tx_vec.len()) {
                                Some(idx) => idx,
                                None => {
                                    log::trace!("Failed to route packet {:?}", packet);
                                    let trashed_frame_bufmut = packet.buffer.into_inner().as_mut_buf();
                                    frame_bufmut_vec.push(trashed_frame_bufmut);
                                    inc_routing_drop();
                                    continue 'packet_drain;
                                }
                            };
                            router_dest_dist[dest_idx] += 1;
                            let _ = &packet_tx_vec[dest_idx]
                                .send(packet)
                                .unwrap_or_else(|_packet| panic!("failed to send packet to {dest_idx} ring is full, distr:{:?}", router_dest_dist));
                            inc_routing_send(&router_dest_label_vec[dest_idx]);
                        }
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        // Only when we drained all events for this poll iteration, we process the next event or break
                        match ev_iter.next() {
                            Some(next_ev) => {
                                ev = next_ev;
                                continue 'drain_readiness_loop;
                            }
                            None => {
                                break 'drain_readiness_loop;
                            }
                        }
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn recv_from(
    available_frame_buf_vec: &mut Vec<FrameBufMut>,
    socket: &UdpSocket,
    batch: &mut Vec<TritonPacket>,
    exit: &AtomicBool,
) -> std::io::Result<usize> {
    // let mut i: usize = 0;
    //DOCUMENTED SIDE-EFFECT
    //Performance out of the IO without poll
    //  * block on the socket until it's readable
    //  * set the socket to non blocking
    //  * read until it fails
    //  * set it back to blocking before returning
    // socket.set_nonblocking(false)?;
    let batch_capacity = batch.capacity();
    assert!(batch_capacity >= PACKETS_PER_BATCH);

    let mut i = 0;

    while !exit.load(Ordering::Relaxed) {
        let npkts = match triton_recv_mmsg(socket, available_frame_buf_vec, batch) {
            Ok(npkts) => npkts,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Drain complete for now. Preserve packets already received in this call.
                return Ok(i);
            }
            Err(e) => return Err(e),
        };
        i += npkts;
        if available_frame_buf_vec.is_empty() {
            break;
        }
        if batch.len() >= batch_capacity {
            break;
        }
        // Try to batch into big enough buffers
        // will cause less re-shuffling later on.
        if i >= PACKETS_PER_BATCH {
            break;
        }
    }
    Ok(i)
}

#[derive(Debug)]
#[repr(C)]
pub struct TritonPacket {
    pub buffer: FrameBuf,
    pub meta: Meta,
}

impl TritonPacket {
    pub fn new(buffer: FrameBuf) -> Self {
        Self {
            buffer,
            meta: Meta::default(),
        }
    }

    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }
}

impl AsRef<[u8]> for TritonPacket {
    fn as_ref(&self) -> &[u8] {
        self.buffer.chunk()
    }
}

pub fn triton_recv_mmsg(
    sock: &UdpSocket,
    fill_buffers: &mut Vec<FrameBufMut>,
    packets: &mut Vec<TritonPacket>,
) -> io::Result</*num packets:*/ usize> {
    const RECV_BURST_TARGET: usize = 32;
    // Should never hit this, but bail if the caller didn't provide any Packets
    // to receive into
    if fill_buffers.is_empty() {
        return Ok(0);
    }
    // Assert that there are no leftovers in packets.
    const SOCKADDR_STORAGE_SIZE: socklen_t = mem::size_of::<sockaddr_storage>() as socklen_t;
    let mut iovs = [MaybeUninit::uninit(); RECV_BURST_TARGET];
    let mut addrs = [MaybeUninit::zeroed(); RECV_BURST_TARGET];
    let mut hdrs = [MaybeUninit::uninit(); RECV_BURST_TARGET];
    let remaining_packets = packets.capacity() - packets.len();
    let sock_fd = sock.as_raw_fd();
    let count = cmp::min(iovs.len(), remaining_packets).min(fill_buffers.len());

    if count == 0 {
        return Ok(0);
    }

    let mut frame_buffer_inflight_vec: [MaybeUninit<FrameBufMut>; RECV_BURST_TARGET] =
        std::array::from_fn(|_| MaybeUninit::uninit());

    let mut frame_buffer_inflight_cnt = 0;
    for (hdr, iov, addr) in izip!(&mut hdrs, &mut iovs, &mut addrs).take(count) {
        let buffer = fill_buffers.pop().expect("insufficient fill buffers");
        assert!(
            buffer.remaining_mut() >= PACKET_DATA_SIZE,
            "fill buffer too small"
        );
        let iov_base = unsafe { buffer.as_mut_ptr() as *mut libc::c_void };

        iov.write(iovec {
            iov_base: iov_base,
            iov_len: PACKET_DATA_SIZE,
        });

        let msg_hdr = create_msghdr(addr, SOCKADDR_STORAGE_SIZE, iov);

        hdr.write(mmsghdr {
            msg_len: 0,
            msg_hdr,
        });
        // Keep track of the in-flight frame buffers to avoid use-after-free
        frame_buffer_inflight_vec[frame_buffer_inflight_cnt].write(buffer);
        frame_buffer_inflight_cnt += 1;
    }

    let mut ts = libc::timespec {
        tv_sec: 1,
        tv_nsec: 0,
    };
    // TODO: remove .try_into().unwrap() once rust libc fixes recvmmsg types for musl 
    #[allow(clippy::useless_conversion)]
    let nrecv = unsafe {
        libc::recvmmsg(
            sock_fd,
            hdrs[0].assume_init_mut(),
            count as u32,
            MSG_DONTWAIT.try_into().unwrap(),
            &mut ts,
        )
    };
    let nrecv = if nrecv < 0 {
        // On error, return all in-flight frame buffers back to the caller
        for i in 0..frame_buffer_inflight_cnt {
            let buffer = unsafe { frame_buffer_inflight_vec[i].assume_init_read() };
            fill_buffers.push(buffer);
        }
        return Err(io::Error::last_os_error());
    } else {
        usize::try_from(nrecv).unwrap()
    };


    for idx in 0..nrecv {
        // SAFETY: `nrecv <= count` and we initialized `count` entries in `hdrs`.
        let hdr_ref = unsafe { hdrs[idx].assume_init_ref() };
        // SAFETY: Same argument as above for `addrs`.
        let addr_ref = unsafe { addrs[idx].assume_init_ref() };
        let mut filled_bufmut = unsafe { frame_buffer_inflight_vec[idx].assume_init_read() };
        unsafe { filled_bufmut.seek(hdr_ref.msg_len as usize); }
        let filled_buf: FrameBuf = filled_bufmut.into();
        let mut pkt = TritonPacket {
            buffer: filled_buf,
            meta: Meta::default(),
        };
        pkt.meta_mut().size = hdr_ref.msg_len as usize;
        if let Some(addr) = cast_socket_addr(addr_ref, hdr_ref) {
            pkt.meta_mut().set_socket_addr(&addr);
        }
        packets.push(pkt);
    }

    if nrecv != count {
        log::debug!(
            "triton_recv_mmsg: recvd {nrecv} packets, expected up to {count}. Remaining fill buffers returned to caller."
        ); 
    }

    // // Return submitted buffers that were not filled by this syscall.
    for in_flight in &mut frame_buffer_inflight_vec[nrecv..count]
    {
        let buffer = unsafe { in_flight.assume_init_read() };
        fill_buffers.push(buffer);
    }

    for (iov, addr, hdr) in izip!(&mut iovs, &mut addrs, &mut hdrs).take(count) {
        // SAFETY: We initialized `count` elements of each array above
        //
        // It may be that `packets.len() != NUM_RCVMMSGS`; thus, some elements
        // in `iovs` / `addrs` / `hdrs` may not get initialized. So, we must
        // manually drop `count` elements from each array instead of being able
        // to convert [MaybeUninit<T>] to [T] and letting `Drop` do the work
        // for us when these items go out of scope at the end of the function
        unsafe {
            iov.assume_init_drop();
            addr.assume_init_drop();
            hdr.assume_init_drop();
        }
    }

    Ok(nrecv)
}

fn create_msghdr(
    msg_name: &mut MaybeUninit<sockaddr_storage>,
    msg_namelen: socklen_t,
    iov: &mut MaybeUninit<iovec>,
) -> msghdr {
    // Cannot construct msghdr directly on musl
    // See https://github.com/rust-lang/libc/issues/2344 for more info
    let mut msg_hdr: msghdr = unsafe { zeroed() };
    msg_hdr.msg_name = msg_name.as_mut_ptr() as *mut _;
    msg_hdr.msg_namelen = msg_namelen;
    msg_hdr.msg_iov = iov.as_mut_ptr();
    msg_hdr.msg_iovlen = 1;
    msg_hdr.msg_control = std::ptr::null::<libc::c_void>() as *mut _;
    msg_hdr.msg_controllen = 0;
    msg_hdr.msg_flags = 0;
    msg_hdr
}

fn cast_socket_addr(addr: &sockaddr_storage, hdr: &mmsghdr) -> Option<SocketAddr> {
    use libc::{sa_family_t, sockaddr_in, sockaddr_in6};
    const SOCKADDR_IN_SIZE: usize = std::mem::size_of::<sockaddr_in>();
    const SOCKADDR_IN6_SIZE: usize = std::mem::size_of::<sockaddr_in6>();
    if addr.ss_family == AF_INET as sa_family_t
        && hdr.msg_hdr.msg_namelen == SOCKADDR_IN_SIZE as socklen_t
    {
        // ref: https://github.com/rust-lang/socket2/blob/65085d9dff270e588c0fbdd7217ec0b392b05ef2/src/sockaddr.rs#L167-L172
        let addr = unsafe { &*(addr as *const _ as *const sockaddr_in) };
        return Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
            u16::from_be(addr.sin_port),
        )));
    }
    if addr.ss_family == AF_INET6 as sa_family_t
        && hdr.msg_hdr.msg_namelen == SOCKADDR_IN6_SIZE as socklen_t
    {
        // ref: https://github.com/rust-lang/socket2/blob/65085d9dff270e588c0fbdd7217ec0b392b05ef2/src/sockaddr.rs#L174-L189
        let addr = unsafe { &*(addr as *const _ as *const sockaddr_in6) };
        return Some(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(addr.sin6_addr.s6_addr),
            u16::from_be(addr.sin6_port),
            addr.sin6_flowinfo,
            addr.sin6_scope_id,
        )));
    }
    error!(
        "recvmmsg unexpected ss_family:{} msg_namelen:{}",
        addr.ss_family, hdr.msg_hdr.msg_namelen
    );
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use libc::{sa_family_t, sockaddr_in, sockaddr_in6};
    use std::thread;

    #[test]
    fn test_create_msghdr_fields() {
        let mut addr = MaybeUninit::<sockaddr_storage>::zeroed();
        let mut iov = MaybeUninit::<iovec>::uninit();
        let namelen = std::mem::size_of::<sockaddr_storage>() as socklen_t;
        let hdr = create_msghdr(&mut addr, namelen, &mut iov);

        assert_eq!(hdr.msg_name, addr.as_mut_ptr() as *mut _);
        assert_eq!(hdr.msg_namelen, namelen);
        assert_eq!(hdr.msg_iov, iov.as_mut_ptr());
        assert_eq!(hdr.msg_iovlen, 1);
    }

    #[test]
    fn test_cast_socket_addr_ipv4() {
        let ip = Ipv4Addr::new(1, 2, 3, 4);
        let port = 12345u16;

        let mut storage: sockaddr_storage = unsafe { zeroed() };
        let sin = sockaddr_in {
            sin_family: AF_INET as sa_family_t,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip.octets()),
            },
            sin_zero: [0; 8],
        };
        unsafe {
            std::ptr::write(&mut storage as *mut _ as *mut sockaddr_in, sin);
        }

        let mut hdr: mmsghdr = unsafe { zeroed() };
        hdr.msg_hdr.msg_namelen = std::mem::size_of::<sockaddr_in>() as socklen_t;

        let out = cast_socket_addr(&storage, &hdr);
        assert_eq!(out, Some(SocketAddr::V4(SocketAddrV4::new(ip, port))));
    }

    #[test]
    fn test_cast_socket_addr_ipv6() {
        let ip = Ipv6Addr::LOCALHOST;
        let port = 54321u16;
        let flowinfo = 7u32;
        let scope_id = 9u32;

        let mut storage: sockaddr_storage = unsafe { zeroed() };
        let sin6 = sockaddr_in6 {
            sin6_family: AF_INET6 as sa_family_t,
            sin6_port: port.to_be(),
            sin6_flowinfo: flowinfo,
            sin6_addr: libc::in6_addr {
                s6_addr: ip.octets(),
            },
            sin6_scope_id: scope_id,
        };
        unsafe {
            std::ptr::write(&mut storage as *mut _ as *mut sockaddr_in6, sin6);
        }

        let mut hdr: mmsghdr = unsafe { zeroed() };
        hdr.msg_hdr.msg_namelen = std::mem::size_of::<sockaddr_in6>() as socklen_t;

        let out = cast_socket_addr(&storage, &hdr);
        assert_eq!(
            out,
            Some(SocketAddr::V6(SocketAddrV6::new(ip, port, flowinfo, scope_id)))
        );
    }

    #[test]
    fn test_cast_socket_addr_invalid_returns_none() {
        let storage: sockaddr_storage = unsafe { zeroed() };
        let mut hdr: mmsghdr = unsafe { zeroed() };
        hdr.msg_hdr.msg_namelen = 0;
        assert_eq!(cast_socket_addr(&storage, &hdr), None);
    }

    #[test]
    fn test_hash_pair_is_deterministic_and_mixes_inputs() {
        let h1 = hash_pair(123, 456);
        let h2 = hash_pair(123, 456);
        let h3 = hash_pair(124, 456);
        let h4 = hash_pair(123, 457);
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        assert_ne!(h1, h4);
    }

    #[test]
    fn test_triton_recv_mmsg_with_udp_socket() {
        let recv_sock = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => return,
            Err(e) => panic!("failed to bind recv socket: {e}"),
        };
        let send_sock = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => return,
            Err(e) => panic!("failed to bind send socket: {e}"),
        };
        let recv_addr = recv_sock.local_addr().unwrap();
        let payload = b"hello-recv-mmsg";

        send_sock.send_to(payload, recv_addr).unwrap();

        let shmem = crate::mem::SharedMem::new(PACKET_DATA_SIZE, 1, false).unwrap();
        let mut fill_buffers = vec![FrameDesc {
            ptr: shmem.ptr,
            frame_size: PACKET_DATA_SIZE,
            shmem_idx: 0,
        }
        .as_mut_buf()];
        let mut packets = Vec::<TritonPacket>::with_capacity(NUM_RCVMMSGS);

        let mut recv_count = 0usize;
        for _ in 0..30 {
            match triton_recv_mmsg(&recv_sock, &mut fill_buffers, &mut packets) {
                Ok(n) => {
                    recv_count = n;
                    if n > 0 {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("triton_recv_mmsg failed: {e}"),
            }
            thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(recv_count, 1);
        assert_eq!(packets.len(), 1);
        let pkt = &packets[0];
        assert_eq!(pkt.meta.size, payload.len());
        assert_eq!(&pkt.buffer.chunk()[..payload.len()], payload);
    }

    #[test]
    fn test_triton_recv_mmsg_returns_unused_buffers() {
        let recv_sock = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => panic!("skipping test_triton_recv_mmsg_returns_unused_buffers due to lack of permissions to bind UDP socket"),
            Err(e) => panic!("failed to bind recv socket: {e}"),
        };
        let send_sock = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => panic!("skipping test_triton_recv_mmsg_returns_unused_buffers due to lack of permissions to bind UDP socket"),
            Err(e) => panic!("failed to bind send socket: {e}"),
        };
        let recv_addr = recv_sock.local_addr().unwrap();
        let payload = b"one-packet-only";
        send_sock.send_to(payload, recv_addr).unwrap();

        let shmem = crate::mem::SharedMem::new(PACKET_DATA_SIZE, 2, false).unwrap();
        let mut fill_buffers = vec![
            FrameDesc {
                ptr: shmem.ptr,
                frame_size: PACKET_DATA_SIZE,
                shmem_idx: 0,
            }
            .as_mut_buf(),
            FrameDesc {
                ptr: unsafe { shmem.ptr.add(PACKET_DATA_SIZE) },
                frame_size: PACKET_DATA_SIZE,
                shmem_idx: 0,
            }
            .as_mut_buf(),
        ];
        let mut packets = Vec::<TritonPacket>::with_capacity(NUM_RCVMMSGS);

        let mut recv_count = 0usize;
        for _ in 0..30 {
            match triton_recv_mmsg(&recv_sock, &mut fill_buffers, &mut packets) {
                Ok(n) => {
                    recv_count = n;
                    if n > 0 {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("triton_recv_mmsg failed: {e}"),
            }
            thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(recv_count, 1);
        assert_eq!(packets.len(), 1);
        // One buffer used by the received packet, one should be returned.
        assert_eq!(fill_buffers.len(), 1);
    }
}

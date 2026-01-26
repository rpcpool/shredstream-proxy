use std::{
    cmp,
    io,
    mem::{self, zeroed, MaybeUninit},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket},
    os::fd::AsRawFd,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut};
use itertools::izip;
use libc::{AF_INET, AF_INET6, MSG_DONTWAIT, iovec, mmsghdr, msghdr, sockaddr_storage};
use log::{error, trace};
use mio::Poll;
use socket2::socklen_t;
use solana_perf::packet::{NUM_RCVMMSGS, PACKETS_PER_BATCH};
use solana_sdk::packet::{Meta, PACKET_DATA_SIZE};
use solana_streamer::{streamer::StreamerReceiveStats};

use crate::{mem::{FrameBuf, FrameBufMut, FrameDesc, Rx, Tx}, prom::{inc_packets_received, observe_recv_packet_count}};

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
    router: R,
) -> std::io::Result<()>
where
    R: PacketRoutingStrategy,
{
    let mut packet_batch = Vec::with_capacity(PACKETS_PER_BATCH);
    let mut frame_bufmut_vec = Vec::with_capacity(PACKETS_PER_BATCH);
    let mut next_stats_report = Instant::now() + Duration::from_secs(1);
    let mut router_dest_dist = vec![0usize; packet_tx_vec.len()];
    let mut poll = Poll::new()?;
    let mut events = mio::Events::with_capacity(sk_vec.len());

    // Initial registration of sockets
    for (i, socket) in sk_vec.iter().enumerate() {
        poll.registry().register(
            &mut mio::net::UdpSocket::from_std(socket.try_clone().unwrap()),
            mio::Token(i),
            mio::Interest::READABLE,
        )?;
    }

    while !exit.load(Ordering::Relaxed) {

        // Events are always cleared before receiving new ones
        let result = poll.poll(&mut events, Some(Duration::from_millis(100)));

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
            
            let sk_idx = ev.token().0;
            let recv_sk = &sk_vec[sk_idx];

            // Refill the frame buffers as much as we can,
            'fill_bufmut: while frame_bufmut_vec.len() < PACKETS_PER_BATCH {
                let maybe_frame_buf = fill_rx.try_recv();
                match maybe_frame_buf {
                    Some(frame_desc) => {
                        let frame_bufmut = frame_desc.as_mut_buf();
                        frame_bufmut_vec.push(frame_bufmut);
                    }
                    None => {
                        if frame_bufmut_vec.is_empty() {
                            // block until we get at least one frame buffer
                            let Some(frame_desc) = fill_rx.recv_timeout(Duration::from_millis(100)) else {
                                break 'fill_bufmut
                            };
                            let frame_bufmut = frame_desc.as_mut_buf();
                            frame_bufmut_vec.push(frame_bufmut);
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

            log::trace!("frame bufmut_vec length: {}", frame_bufmut_vec.len());
            
            let t = Instant::now();
            let result = recv_from(&mut frame_bufmut_vec, recv_sk, &mut packet_batch, &exit);
            let recv_interval = t.elapsed();


            match result {
                Ok(len) => {
                    log::trace!("recv_from got {} packets in {:?}", len, recv_interval);
                    if len > 0 {
                        // observe_recv_interval(recv_interval.as_micros() as f64);
                        log::trace!("Received {} packets", len);
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
                                    log::debug!("Failed to route packet {:?}", packet);
                                    let trashed_frame_bufmut = packet.buffer.into_inner().as_mut_buf();
                                    frame_bufmut_vec.push(trashed_frame_bufmut);
                                    continue 'packet_drain;
                                }
                            };
                            router_dest_dist[dest_idx] += 1;
                            let _ = &packet_tx_vec[dest_idx]
                                .send(packet)
                                .expect(format!("failed to send packet to {dest_idx} ring is full, distr:{:?}", router_dest_dist).as_str());
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
    trace!("receiving on {}", socket.local_addr().unwrap());
    let batch_capacity = batch.capacity();
    assert!(batch_capacity >= PACKETS_PER_BATCH);

    let mut i = 0;

    while !exit.load(Ordering::Relaxed) {
        log::trace!("Preparing to receive packets, currently have {} packets", i);
        let npkts = triton_recv_mmsg(socket, available_frame_buf_vec, batch)?;
        trace!("got {} packets", npkts);
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
    // Should never hit this, but bail if the caller didn't provide any Packets
    // to receive into
    if fill_buffers.is_empty() {
        log::trace!("triton_recv_mmsg: no fill buffers to receive into");
        return Ok(0);
    }
    // Assert that there are no leftovers in packets.
    const SOCKADDR_STORAGE_SIZE: socklen_t = mem::size_of::<sockaddr_storage>() as socklen_t;

    let mut iovs = [MaybeUninit::uninit(); NUM_RCVMMSGS];
    let mut addrs = [MaybeUninit::zeroed(); NUM_RCVMMSGS];
    let mut hdrs = [MaybeUninit::uninit(); NUM_RCVMMSGS];
    let remaining_packets = packets.capacity() - packets.len();
    let sock_fd = sock.as_raw_fd();
    let count = cmp::min(iovs.len(), remaining_packets).min(fill_buffers.len());
    log::trace!(
        "triton_recv_mmsg: preparing to receive up to {} packets",
        count
    );
    let mut frame_buffer_inflight_vec: [MaybeUninit<FrameBufMut>; NUM_RCVMMSGS] =
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
    log::trace!("Calling recvmmsg with count={}", count);
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
    log::trace!("recvmmsg returned nrecv={}", nrecv);
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
    for (addr, hdr, filled_bufmut) in
        izip!(addrs, hdrs, frame_buffer_inflight_vec).take(nrecv)
    {
        // SAFETY: We initialized `count` elements of `hdrs` above. `count` is
        // passed to recvmmsg() as the limit of messages that can be read. So,
        // `nrevc <= count` which means we initialized this `hdr` and
        // recvmmsg() will have updated it appropriately
        let hdr_ref = unsafe { hdr.assume_init_ref() };
        // SAFETY: Similar to above, we initialized this `addr` and recvmmsg()
        // will have populated it
        let addr_ref = unsafe { addr.assume_init_ref() };
        let mut filled_bufmut = unsafe { filled_bufmut.assume_init_read() };
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

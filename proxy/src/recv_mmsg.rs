use std::{
    cmp,
    collections::VecDeque,
    hint::spin_loop,
    io,
    mem::{self, zeroed, MaybeUninit},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket},
    os::fd::AsRawFd,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut};
use itertools::izip;
use libc::{iovec, mmsghdr, msghdr, sockaddr_storage, AF_INET, AF_INET6, MSG_WAITFORONE};
use log::{error, trace};
use socket2::socklen_t;
use solana_ledger::shred::ShredId;
use solana_perf::packet::{NUM_RCVMMSGS, PACKETS_PER_BATCH};
use solana_sdk::packet::{Meta, Packet, PACKET_DATA_SIZE};
use solana_streamer::{recvmmsg::recv_mmsg, streamer::StreamerReceiveStats};

use crate::mem::{try_alloc_shared_mem, FrameBuf, FrameBufMut, FrameDesc, Rx, SharedMem, Tx};

const OFFSET_SHRED_TYPE: usize = 82;
const OFFSET_DATA_PARENT: usize = 83; // 83 + 0
const OFFSET_DATA_INDEX: usize = 83 - 15; // Index is actually in common header
const OFFSET_CODING_POSITION: usize = 83 + 2;

// Shred types based on Solana spec
const SHRED_TYPE_DATA: u8 = 0b1010_0101;
const SHRED_TYPE_CODING: u8 = 0b0101_1010;

pub struct RecvMemConfig {
    pub frames_count: usize,
    pub hugepages: bool,
}

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
    socket: &UdpSocket,
    exit: &AtomicBool,
    stats: &StreamerReceiveStats,
    coalesce: Duration,
    fill_rx: &mut Rx<FrameDesc>,
    packet_tx_vec: &[Tx<TritonPacket>],
    router: R,
) -> std::io::Result<()>
where
    R: PacketRoutingStrategy,
{
    let mut packet_batch = Vec::with_capacity(PACKETS_PER_BATCH);
    let mut frame_bufmut_vec = Vec::with_capacity(PACKETS_PER_BATCH);

    loop {
        // Check for exit signal, even if socket is busy
        // (for instance the leader transaction socket)
        if exit.load(Ordering::Relaxed) {
            return Ok(());
        }

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
                        let frame_desc = fill_rx.recv();
                        let frame_bufmut = frame_desc.as_mut_buf();
                        frame_bufmut_vec.push(frame_bufmut);
                    } else {
                        break 'fill_bufmut;
                    }
                }
            }
        }
        let result = recv_from(&mut frame_bufmut_vec, socket, coalesce, &mut packet_batch);
        if let Ok(len) = result {
            if len > 0 {
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

                for packet in packet_batch.drain(..) {
                    let dest_idx = match router.route_packet(&packet, packet_tx_vec.len()) {
                        Some(idx) => idx,
                        None => {
                            log::debug!("Failed to route packet {:?}", packet);
                            let trashed_frame_bufmut = packet.buffer.into_inner().as_mut_buf();
                            frame_bufmut_vec.push(trashed_frame_bufmut);
                            continue;
                        }
                    };
                    let _ = &packet_tx_vec[dest_idx]
                        .send(packet)
                        .expect("Failed to send packet to processor");
                }
            }
        }
    }
}

pub fn recv_from(
    available_frame_buf_vec: &mut Vec<FrameBufMut>,
    socket: &UdpSocket,
    max_wait: Duration,
    batch: &mut Vec<TritonPacket>,
) -> std::io::Result<usize> {
    // let mut i: usize = 0;
    //DOCUMENTED SIDE-EFFECT
    //Performance out of the IO without poll
    //  * block on the socket until it's readable
    //  * set the socket to non blocking
    //  * read until it fails
    //  * set it back to blocking before returning
    socket.set_nonblocking(false)?;
    trace!("receiving on {}", socket.local_addr().unwrap());
    let start = Instant::now();

    assert!(batch.capacity() >= PACKETS_PER_BATCH);

    let mut i = 0;

    loop {
        match triton_recv_mmsg(socket, available_frame_buf_vec, &mut batch[i..]) {
            Err(_) if i > 0 => {
                if start.elapsed() > max_wait {
                    break;
                }
            }
            Err(e) => {
                trace!("recv_from err {:?}", e);
                return Err(e);
            }
            Ok(npkts) => {
                if i == 0 {
                    socket.set_nonblocking(true)?;
                }
                trace!("got {} packets", npkts);
                i += npkts;
                // Try to batch into big enough buffers
                // will cause less re-shuffling later on.
                if start.elapsed() > max_wait || i >= PACKETS_PER_BATCH {
                    break;
                }
            }
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
    packets: &mut [TritonPacket],
) -> io::Result</*num packets:*/ usize> {
    // Should never hit this, but bail if the caller didn't provide any Packets
    // to receive into
    if packets.is_empty() {
        return Ok(0);
    }
    // Assert that there are no leftovers in packets.
    const SOCKADDR_STORAGE_SIZE: socklen_t = mem::size_of::<sockaddr_storage>() as socklen_t;

    let mut iovs = [MaybeUninit::uninit(); NUM_RCVMMSGS];
    let mut addrs = [MaybeUninit::zeroed(); NUM_RCVMMSGS];
    let mut hdrs = [MaybeUninit::uninit(); NUM_RCVMMSGS];

    let sock_fd = sock.as_raw_fd();
    let count = cmp::min(iovs.len(), packets.len()).min(fill_buffers.len());

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
    #[allow(clippy::useless_conversion)]
    let nrecv = unsafe {
        libc::recvmmsg(
            sock_fd,
            hdrs[0].assume_init_mut(),
            count as u32,
            MSG_WAITFORONE.try_into().unwrap(),
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
    for (addr, hdr, pkt, filled_bufmut) in
        izip!(addrs, hdrs, packets.iter_mut(), frame_buffer_inflight_vec).take(nrecv)
    {
        // SAFETY: We initialized `count` elements of `hdrs` above. `count` is
        // passed to recvmmsg() as the limit of messages that can be read. So,
        // `nrevc <= count` which means we initialized this `hdr` and
        // recvmmsg() will have updated it appropriately
        let hdr_ref = unsafe { hdr.assume_init_ref() };
        // SAFETY: Similar to above, we initialized this `addr` and recvmmsg()
        // will have populated it
        let addr_ref = unsafe { addr.assume_init_ref() };
        let filled_bufmut = unsafe { filled_bufmut.assume_init_read() };
        let filled_buf: FrameBuf = filled_bufmut.into();
        pkt.buffer = filled_buf;
        pkt.meta_mut().size = hdr_ref.msg_len as usize;
        if let Some(addr) = cast_socket_addr(addr_ref, hdr_ref) {
            pkt.meta_mut().set_socket_addr(&addr);
        }
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

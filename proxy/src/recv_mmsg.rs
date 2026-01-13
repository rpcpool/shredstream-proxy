use bytes::BufMut;
use itertools::izip;
use libc::{AF_INET, AF_INET6, MSG_WAITFORONE, iovec, mmsghdr, msghdr, sockaddr_storage};
use socket2::socklen_t;
use solana_perf::packet::{NUM_RCVMMSGS, PACKETS_PER_BATCH};
use solana_sdk::packet::{Meta, PACKET_DATA_SIZE, Packet};
use log::{error, trace};
use solana_streamer::{recvmmsg::recv_mmsg, streamer::StreamerReceiveStats};
use std::{
    cmp, collections::VecDeque, io, mem::{self, MaybeUninit, zeroed}, net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket}, os::fd::AsRawFd, sync::atomic::{AtomicBool, Ordering}, time::{Duration, Instant}
};

use crate::mem::{FrameBuffer, FrameDesc, PagedAlignedMem, Rx, Tx, try_alloc_shared_mem};


pub struct RecvMemConfig {
    pub frames_count: usize,
    pub hugepages: bool,
}


fn recv_loop(
    socket: &UdpSocket,
    exit: &AtomicBool,
    stats: &StreamerReceiveStats,
    coalesce: Duration,
    mem_config: &RecvMemConfig,
) -> std::io::Result<()> {
    
    let data_shmem = try_alloc_shared_mem(PACKET_DATA_SIZE.next_power_of_two(), mem_config.frames_count, mem_config.hugepages).expect("try_alloc_shared_mem");
    let mut packet_batch = Vec::with_capacity(PACKETS_PER_BATCH);
    loop {
        // Check for exit signal, even if socket is busy
        // (for instance the leader transaction socket)
        if exit.load(Ordering::Relaxed) {
            return Ok(());
        }


        if let Ok(len) = recv_from(&mut packet_batch, socket, coalesce) {
            if len > 0 {
                let StreamerReceiveStats {
                    packets_count,
                    packet_batches_count,
                    full_packet_batches_count,
                    max_channel_len,
                    ..
                } = stats;

                packets_count.fetch_add(len, Ordering::Relaxed);
                packet_batches_count.fetch_add(1, Ordering::Relaxed);
                max_channel_len.fetch_max(packet_batch_sender.len(), Ordering::Relaxed);
                if len == PACKETS_PER_BATCH {
                    full_packet_batches_count.fetch_add(1, Ordering::Relaxed);
                }
                packet_batch
                    .iter_mut()
                    .for_each(|p| p.meta_mut().set_from_staked_node(is_staked_service));
                packet_batch_sender.send(packet_batch)?;
            }
            break;
        }
    }
}



pub fn recv_from(
    fill_ring_rx: &mut Rx<FrameDesc>,
    fill_ring_tx: &Tx<FrameDesc>,
    socket: &UdpSocket, 
    max_wait: Duration,
    batch: &mut Vec<TritonPacket>
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

    struct Defer<'a> {
        i: usize,
        allocated_frame: usize,
        batch: &'a mut Vec<TritonPacket>,
    };

    impl Drop for Defer<'_> {
        fn drop(&mut self) {
            // Return unused frames to the fill ring
            let exceeding_allocs = self.allocated_frame.saturating_sub(self.i);
            (0..exceeding_allocs).for_each(|_| {
                if let Some(unused_buffer) = self.batch.pop() {
                    drop(unused_buffer);
                }
            });
            self.allocated_frame = 0;
        }
    }

    let mut defer = Defer {
        i: 0,
        allocated_frame: 0,
        batch,
    };

    loop {

        let frame_desc = fill_ring_rx.recv();
        let buffer = FrameBuffer::new(frame_desc, fill_ring_tx.clone());
        defer.allocated_frame += 1;
        defer.batch[defer.i] = TritonPacket::new(buffer);

        let mut j = defer.i + 1;
        while j < PACKETS_PER_BATCH {

            let Some(frame_desc) = fill_ring_rx.try_recv() else {
                break;
            };
            let buffer = FrameBuffer::new(frame_desc, fill_ring_tx.clone());
            defer.batch[j] = TritonPacket::new(buffer);
            defer.allocated_frame += 1;
            j += 1;

        }

        match triton_recv_mmsg(socket, &mut defer.batch[defer.i..j]) {
            Err(_) if defer.i > 0 => {
                if start.elapsed() > max_wait {
                    break;
                }
            }
            Err(e) => {
                trace!("recv_from err {:?}", e);
                return Err(e);
            }
            Ok(npkts) => {
                if defer.i == 0 {
                    socket.set_nonblocking(true)?;
                }
                trace!("got {} packets", npkts);
                defer.i += npkts;
                // Try to batch into big enough buffers
                // will cause less re-shuffling later on.
                if start.elapsed() > max_wait || defer.i >= PACKETS_PER_BATCH {
                    break;
                }
            }
        }
    }

    Ok(defer.i)
}

pub struct TritonPacket {
    pub buffer: FrameBuffer,
    pub meta: Meta,
}

impl TritonPacket {
    pub fn new(buffer: FrameBuffer) -> Self {
        Self {
            buffer,
            meta: Meta::default(),
        }
    }

    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }
}


pub fn triton_recv_mmsg(sock: &UdpSocket, packets: &mut [TritonPacket]) -> io::Result</*num packets:*/ usize> {
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
    let count = cmp::min(iovs.len(), packets.len());

    for (packet, hdr, iov, addr) in
        izip!(packets.iter_mut(), &mut hdrs, &mut iovs, &mut addrs).take(count)
    {
        let buffer = packet.buffer.base();
        iov.write(iovec {
            iov_base: buffer as *mut libc::c_void,
            iov_len: PACKET_DATA_SIZE,
        });

        let msg_hdr = create_msghdr(addr, SOCKADDR_STORAGE_SIZE, iov);

        hdr.write(mmsghdr {
            msg_len: 0,
            msg_hdr,
        });
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
        return Err(io::Error::last_os_error());
    } else {
        usize::try_from(nrecv).unwrap()
    };
    for (addr, hdr, pkt) in izip!(addrs, hdrs, packets.iter_mut()).take(nrecv) {
        // SAFETY: We initialized `count` elements of `hdrs` above. `count` is
        // passed to recvmmsg() as the limit of messages that can be read. So,
        // `nrevc <= count` which means we initialized this `hdr` and
        // recvmmsg() will have updated it appropriately
        let hdr_ref = unsafe { hdr.assume_init_ref() };
        // SAFETY: Similar to above, we initialized this `addr` and recvmmsg()
        // will have populated it
        let addr_ref = unsafe { addr.assume_init_ref() };
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

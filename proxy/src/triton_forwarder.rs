use std::{
    collections::VecDeque, net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket}, num::NonZeroUsize, str::FromStr, sync::{
        Arc, atomic::{AtomicBool, Ordering}
    }, thread::JoinHandle, time::{Duration, Instant}
};

use arc_swap::ArcSwap;
use bytes::Buf;
use crossbeam_channel::{Receiver, Sender};
use itertools::{izip, Itertools};
use libc;
use log::{debug, error, info, warn};
use solana_net_utils::SocketConfig;
use solana_perf::deduper::Deduper;
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer::{StreamerReceiveStats},
};

use crate::{
    forwarder::{ShredMetrics, try_create_ipv6_socket}, mem::{FrameBuf, FrameDesc, Rx, SharedMem, Tx}, triton_multicast_config::TritonMulticastConfig, prom::{
        inc_packets_deduped, inc_packets_forward_failed,
        observe_dedup_time,
        observe_send_duration, observe_send_packet_count,
    }, recv_mmsg::{PacketRoutingStrategy, TritonPacket}
};

// values copied from https://github.com/solana-labs/solana/blob/33bde55bbdde13003acf45bb6afe6db4ab599ae4/core/src/sigverify_shreds.rs#L20
pub const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
pub const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
pub const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);
pub const IP_MULTICAST_TTL: u32 = 8;

#[derive(Debug, Clone, Copy, Default)]
pub enum PktRecvMemSizing {
    #[default]
    XSmall = 134217728, // 128MiB
    Small = 268435456,        // 256MiB
    Medium = 536870912,       // 512MiB
    Large = 1073741824,       // 1GiB
    XLarge = 2147483648,      // 2GiB
    XXLarge = 4294967296,     // 4GiB
    XXXLarge = 8589934592,    // 8GiB
    XXXXLarge = 17179869184,  // 16GiB
    XXXXXLarge = 34359738368, // 32GiB
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid ReceiverMemoryCapacity: {0}")]
pub struct ReceiverMemoryCapacityFromStrErr(String);

impl FromStr for PktRecvMemSizing {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "xsmall" | "xs" => Ok(PktRecvMemSizing::XSmall),
            "small" | "s" => Ok(PktRecvMemSizing::Small),
            "medium" | "m" => Ok(PktRecvMemSizing::Medium),
            "large" | "l" => Ok(PktRecvMemSizing::Large),
            "xlarge" | "xl" => Ok(PktRecvMemSizing::XLarge),
            "xxlarge" | "xxl" | "2xl" => Ok(PktRecvMemSizing::XXLarge),
            "xxxlarge" | "xxxl" | "3xl" => Ok(PktRecvMemSizing::XXXLarge),
            "xxxxlarge" | "xxxxl" | "4xl" => Ok(PktRecvMemSizing::XXXXLarge),
            "xxxxxlarge" | "xxxxxl" | "5xl" => Ok(PktRecvMemSizing::XXXXXLarge),
            _ => Err(s.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PktRecvTileMemConfig {
    pub frame_size: usize,
    pub memory_size: PktRecvMemSizing,
    pub hugepage: bool,
}

impl Default for PktRecvTileMemConfig {
    fn default() -> Self {
        Self {
            frame_size: 2048,
            memory_size: PktRecvMemSizing::default(),
            hugepage: false,
        }
    }
}

fn packet_recv_tile<R>(
    pkt_recv_idx: usize,
    pkt_recv_socket: UdpSocket,
    exit: Arc<AtomicBool>,
    forwarder_stats: Arc<StreamerReceiveStats>,
    mut fill_rx: Rx<FrameDesc>,
    packet_tx_vec: Vec<Tx<TritonPacket>>,
    packet_router: R,
    tile_drop_sig: TileClosedSignal,
) -> std::io::Result<JoinHandle<()>>
where
    R: PacketRoutingStrategy + Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("ssListen{pkt_recv_idx}"))
        .spawn(move || {
            crate::recv_mmsg::recv_loop(
                &pkt_recv_socket,
                &exit,
                &forwarder_stats,
                Duration::default(),
                &mut fill_rx,
                &packet_tx_vec,
                packet_router,
            )
            .expect("recv_loop");
            drop(tile_drop_sig);
        })
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct SharedMemInfo {
    pub start_ptr: *const u8,
    pub len: usize, // always a power of 2
}

unsafe impl Send for SharedMemInfo {}
unsafe impl Sync for SharedMemInfo {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TileKind {
    PktRecv,
    PktFwd,
}

struct TileClosedSignal {
    kind: TileKind,
    idx: usize,
    tx: Option<Sender<(TileKind, usize)>>,
}

struct TileWaitGroup {
    rx: Receiver<(TileKind, usize)>,
    tx: Sender<(TileKind, usize)>,
}

impl TileWaitGroup {
    fn new() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self { rx, tx }
    }

    fn get_tile_closed_signal(&self, kind: TileKind, idx: usize) -> TileClosedSignal {
        TileClosedSignal {
            kind,
            idx,
            tx: Some(self.tx.clone()),
        }
    }

    fn wait_first(self) -> (TileKind, usize) {
        drop(self.tx);
        self.rx.recv().expect("TileWaitGroup::wait_first")
    }
}

impl Drop for TileClosedSignal {
    fn drop(&mut self) {
        if let Some(tx) = &self.tx {
            let _ = tx.send((self.kind, self.idx));
        }
    }
}

fn packet_fwd_tile(
    packet_fwd_idx: usize,
    hot_dest_vec: Arc<ArcSwap<Vec<SocketAddr>>>,
    send_socket: UdpSocket,
    mut packet_rx: Rx<TritonPacket>,
    fill_tx_vec: Vec<Tx<FrameDesc>>,
    shmem_info_vec: Vec<SharedMemInfo>,
    stats: Arc<ShredMetrics>,
    exit: Arc<AtomicBool>,
    tile_drop_sig: TileClosedSignal,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(format!("ssPxyTx_{packet_fwd_idx}"))
        .spawn(move || {
            let mut deduper = Deduper::<2, [u8]>::new(&mut rand::thread_rng(), DEDUPER_NUM_BITS);
            const UIO_MAXIOV: usize = libc::UIO_MAXIOV as usize;
            // We allocate double size to account for possible overflow if destinations array is really big
            let mut next_batch_send: Vec<(FrameBuf, SocketAddr)> = Vec::with_capacity(UIO_MAXIOV);
            let mut queued: VecDeque<TritonPacket> = VecDeque::with_capacity(UIO_MAXIOV);

            for shmem_info in &shmem_info_vec {
                assert!(
                    shmem_info.len.is_power_of_two(),
                    "shmem_info.len must be a power of 2"
                );
            }

            let mut next_deduper_reset_attempt = Instant::now() + Duration::from_secs(2);
            let mut recycled_frames: Vec<FrameDesc> = Vec::with_capacity(UIO_MAXIOV);
            while !exit.load(Ordering::Relaxed) {
                if next_deduper_reset_attempt.elapsed() > Duration::ZERO {
                    deduper.maybe_reset(
                        &mut rand::thread_rng(),
                        DEDUPER_FALSE_POSITIVE_RATE,
                        DEDUPER_RESET_CYCLE,
                    );
                    next_deduper_reset_attempt = Instant::now() + Duration::from_secs(2);
                }

                if queued.is_empty() && recycled_frames.is_empty() && next_batch_send.is_empty() {
                    if let Some(packet) = packet_rx.recv_timeout(Duration::from_millis(100)) {
                        let data_size = packet.meta.size;
                        let data_slice = &packet.buffer.chunk()[..data_size];
                        if deduper.dedup(data_slice) {
                            // put it inside the recycle queue
                            debug!("Deduped packet from {}", packet.meta.addr);
                            let desc = packet.buffer.into_inner();
                            recycled_frames.push(desc);
                        } else {
                            queued.push_back(packet);
                        }
                    }
                }

                // Fill up the queued OR recycled_frames as much as possible
                'fill_backlog: while queued.len() < UIO_MAXIOV && recycled_frames.len() < UIO_MAXIOV {
                    // Fill the batch as much as possible.
                    let Some(packet) = packet_rx.try_recv() else {
                        break 'fill_backlog;
                    };
                    let data_size = packet.meta.size;
                    let data_slice = &packet.buffer.chunk()[..data_size];
                    let t = Instant::now();
                    if deduper.dedup(data_slice) {
                        // put it inside the recycle queue
                        debug!("Deduped packet from {}", packet.meta.addr);
                        let desc = packet.buffer.into_inner();
                        recycled_frames.push(desc);
                        inc_packets_deduped(1);
                    } else {
                        queued.push_back(packet);
                    }
                    let dedup_duration = t.elapsed();
                    observe_dedup_time(dedup_duration.as_micros() as f64);
                }

                let dests = hot_dest_vec.load();
                let dests_len = dests.len();

                // Fill up the next_batch_send
                'fill_batch_send: while next_batch_send.len() < UIO_MAXIOV
                    && queued.len() > 0
                    && dests_len > 0
                {
                    let remaining = UIO_MAXIOV - next_batch_send.len();
                    if dests_len < remaining {
                        break 'fill_batch_send;
                    }

                    let Some(packet) = queued.pop_front() else {
                        break 'fill_batch_send;
                    };
                    let buf = packet.buffer;
                    let desc = unsafe { buf.detach_desc() };
                    recycled_frames.push(desc);

                    for dest in dests.iter() {
                        // Cheap to do since we are just copying a pointer
                        let buf_clone = unsafe { buf.unsafe_subslice_clone(0, packet.meta.size) };
                        next_batch_send.push((buf_clone, *dest));
                    }
                }

                assert!(
                    next_batch_send.len() <= UIO_MAXIOV,
                    "next_batch_send.len() = {}",
                    next_batch_send.len()
                );
                assert!(
                    recycled_frames.len() <= UIO_MAXIOV,
                    "recycled_frames.len() = {}",
                    recycled_frames.len()
                );
                assert!(
                    queued.len() <= UIO_MAXIOV,
                    "queued.len() = {}",
                    queued.len()
                );

                let batch_send_ts = Instant::now();
                match batch_send(&send_socket, &next_batch_send) {
                    Ok(_) => {
                        // Successfully sent all packets in the batch
                        let send_duration = batch_send_ts.elapsed();
                        stats.batch_send_time_spent.fetch_add(send_duration.as_micros() as u64, Ordering::Relaxed);
                        stats.send_batch_count.fetch_add(1, Ordering::Relaxed);
                        observe_send_duration(send_duration.as_micros() as f64);
                        observe_send_packet_count(next_batch_send.len() as f64);
                    }
                    Err(SendPktsError::IoError(err, num_failed)) => {
                        error!(
                            "Failed to send batch of size {}. \
                             {num_failed} packets failed. Error: {err}",
                            next_batch_send.len()
                        );
                        inc_packets_forward_failed(num_failed as u64);
                    }
                }

                next_batch_send.clear();

                // Recycle all used frames
                while let Some(desc) = recycled_frames.pop() {
                    let fill_ring_idx = shmem_info_vec
                        .iter()
                        .find_position(|shmem_info| {
                            (desc.ptr as usize) & (shmem_info.len - 1)
                                == (shmem_info.start_ptr as usize)
                        })
                        .expect("unknown frame desc")
                        .0;
                    fill_tx_vec[fill_ring_idx]
                        .send(desc)
                        .expect("frame recycling");
                }
            }
            log::info!("Exiting pkt_fwd_tile {}", packet_fwd_idx);
            drop(tile_drop_sig);
        })
}

pub fn run_proxy_system<R>(
    pkt_recv_tile_mem_config: PktRecvTileMemConfig,
    dest_addr_vec: Arc<ArcSwap<Vec<SocketAddr>>>,
    multticast_config: Option<TritonMulticastConfig>,
    src_ip: IpAddr,
    src_port: u16,
    num_pkt_recv_tiles: usize,
    num_pkt_fwd_tiles: usize,
    pkt_router: R,
    exit: Arc<AtomicBool>,
    pk_recv_stats: Arc<StreamerReceiveStats>,
    pk_fwd_stats: Arc<ShredMetrics>,
) where
    R: PacketRoutingStrategy + Send + Sync + 'static,
{
    let mut tile_thread_vec: Vec<JoinHandle<()>> = Vec::new();
    // Build pkt_recv sockets
    let pkt_recv_sk_vec = if let Some(multicast_config) = multticast_config {
        log::info!("Using Triton multicast configuration for pkt_recv tiles");
        crate::triton_multicast_config::create_multicast_sockets_triton(
            &multicast_config, 
            NonZeroUsize::new(num_pkt_recv_tiles).expect("num_pkt_recv_tiles must be non-zero"),
            src_ip,
            src_port,
        ).expect("multicast-config")
    } else {
        let (_port, pkt_recv_sk_vec) = solana_net_utils::multi_bind_in_range_with_config(
            src_ip,
            (src_port, src_port + 1),
            SocketConfig::default().reuseport(true),
            num_pkt_recv_tiles,
        )
        .unwrap_or_else(|_| {
            panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
        });
        pkt_recv_sk_vec
    };

    let num_frames =
        pkt_recv_tile_mem_config.memory_size as usize / pkt_recv_tile_mem_config.frame_size;
    let frame_size = pkt_recv_tile_mem_config.frame_size;

    let tile_wait_group = TileWaitGroup::new();
    let mut shmem_info_vec: Vec<SharedMemInfo> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut fill_tx_vec: Vec<Tx<FrameDesc>> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut fill_rx_vec: Vec<Rx<FrameDesc>> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut shmem_vec: Vec<SharedMem> = Vec::with_capacity(num_pkt_recv_tiles);

    let mut pkt_fwd_sk_vec: Vec<UdpSocket> = Vec::with_capacity(num_pkt_fwd_tiles);
    let mut packet_rx_vec: Vec<Rx<TritonPacket>> = Vec::with_capacity(num_pkt_fwd_tiles);
    let mut packet_tx_vec: Vec<Tx<TritonPacket>> = Vec::with_capacity(num_pkt_fwd_tiles);

    // Create the shared memory regions for recv tiles
    for _ in 0..num_pkt_recv_tiles {
        assert!(
            num_frames.is_power_of_two(),
            "num_frames must be a power of 2"
        );
        assert!(
            frame_size.is_power_of_two(),
            "frame_size must be a power of 2"
        );
        let shmem = SharedMem::new(frame_size, num_frames, pkt_recv_tile_mem_config.hugepage)
            .expect("SharedMem::new");
        log::info!(
            "Created shared memory region with frame_size={} num_frames={} total_size={} hugepage={}",
            frame_size,
            num_frames,
            shmem.len(),
            pkt_recv_tile_mem_config.hugepage,
        );

        let shmem_info = SharedMemInfo {
            start_ptr: shmem.ptr,
            len: shmem.len(),
        };
        shmem_info_vec.push(shmem_info);

        let (fill_tx, fill_rx) = crate::mem::message_ring(num_frames).expect("frame ring");
        // Fill the fill ring with all frames
        for i in 0..num_frames {
            let frame_desc = FrameDesc {
                ptr: unsafe { shmem.ptr.add(i * frame_size) },
                frame_size: frame_size,
            };
            fill_tx
                .send(frame_desc)
                .expect("initial frame ring population");
        }
        shmem_vec.push(shmem);
        fill_tx_vec.push(fill_tx);
        fill_rx_vec.push(fill_rx);
        log::info!("Initialized frame ring with {} frames", num_frames);
    }

    // Create socket for sending packets
    for _ in 0..num_pkt_fwd_tiles {
        let send_socket = {
            let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
            match try_create_ipv6_socket(ipv6_addr) {
                Ok(socket) => {
                    info!("Successfully bound send socket to IPv6 dual-stack address.");
                    socket
                        .set_multicast_loop_v6(false)
                        .expect("Failed to disable IPv6 multicast loopback");
                    socket
                }
                Err(e) if e.raw_os_error() == Some(libc::EAFNOSUPPORT) => {
                    // This error (code 97 on Linux) means IPv6 is not supported.
                    warn!("IPv6 not available. Falling back to IPv4-only for sending.");
                    let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
                    let socket = UdpSocket::bind(ipv4_addr)
                        .expect("Failed to bind to IPv4 socket after IPv6 failed");
                    socket
                        .set_multicast_ttl_v4(IP_MULTICAST_TTL)
                        .expect("IP_MULTICAST_TTL_V4");
                    socket
                        .set_multicast_loop_v4(false)
                        .expect("Failed to disable IPv4 multicast loopback");
                    socket
                }
                Err(e) => {
                    // For any other error (e.g., port in use), panic.
                    panic!("Failed to bind send socket with an unexpected error: {e}");
                }
            }
        };
        log::info!(
            "Packet forwarder sending socket bound to {}",
            send_socket.local_addr().unwrap()
        );
        pkt_fwd_sk_vec.push(send_socket);
    }

    // Create pkt_fwd message rings
    // One ring per pkt_fwd tile
    for _ in 0..num_pkt_fwd_tiles {
        let (packet_tx, packet_rx) = crate::mem::message_ring(num_frames).expect("pkt_fwd ring");
        packet_tx_vec.push(packet_tx);
        packet_rx_vec.push(packet_rx);
    }
    log::info!(
        "Initialized pkt_fwd message rings with {} slots",
        num_frames
    );

    // Spawn pkt_fwd tiles
    for (pkt_fwd_idx, pkt_fwd_sk, packet_rx) in izip!(
        0..num_pkt_fwd_tiles,
        pkt_fwd_sk_vec.into_iter(),
        packet_rx_vec.into_iter()
    ) {
        let hot_dest_vec = Arc::clone(&dest_addr_vec);
        let fill_tx_vec = fill_tx_vec.clone();
        let shmem_info_vec = shmem_info_vec.clone();
        let exit = Arc::clone(&exit);
        let th = packet_fwd_tile(
            pkt_fwd_idx,
            hot_dest_vec,
            pkt_fwd_sk,
            packet_rx,
            fill_tx_vec,
            shmem_info_vec,
            Arc::clone(&pk_fwd_stats),
            exit,
            tile_wait_group.get_tile_closed_signal(TileKind::PktFwd, pkt_fwd_idx),
        )
        .expect("packet_fwd_tile");
        tile_thread_vec.push(th);
        log::info!("Spawned pkt_fwd tile {}", pkt_fwd_idx);
    }

    // Spawn pkt_recv tiles
    for (pkt_recv_idx, pkt_recv_sk, fill_rx) in izip!(
        0..num_pkt_recv_tiles,
        pkt_recv_sk_vec.into_iter(),
        fill_rx_vec.into_iter()
    ) {
        let exit = Arc::clone(&exit);
        let forwarder_stats = Arc::clone(&pk_recv_stats);
        let packet_tx_vec_clone = packet_tx_vec.clone();
        let pkt_router_clone = pkt_router.clone();
        let jh = packet_recv_tile(
            pkt_recv_idx,
            pkt_recv_sk,
            exit,
            forwarder_stats,
            fill_rx,
            packet_tx_vec_clone,
            pkt_router_clone,
            tile_wait_group.get_tile_closed_signal(TileKind::PktRecv, pkt_recv_idx),
        )
        .expect("packet_recv_tile");
        tile_thread_vec.push(jh);
        log::info!("Spawned pkt_recv tile {}", pkt_recv_idx);
    }

    let (kind, idx) = tile_wait_group.wait_first();
    warn!("Tile of kind {kind:?} with idx {idx} has exited. Shutting down proxy system");

    exit.store(true, Ordering::Release);
    drop(fill_tx_vec);
    drop(packet_tx_vec);
    log::info!("Waiting for {} tile threads to exit", tile_thread_vec.len());
    for th in tile_thread_vec {
        let result = th.join();
        if let Err(e) = result {
            error!("Tile thread join error: {:?}", e);
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{
//         net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
//         str::FromStr,
//         sync::{Arc, Mutex, RwLock},
//         thread,
//         thread::sleep,
//         time::Duration,
//     };

//     use solana_perf::{
//         deduper::Deduper,
//         packet::{Meta, Packet, PacketBatch},
//     };
//     use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};

//     fn listen_and_collect(listen_socket: UdpSocket, received_packets: Arc<Mutex<Vec<Vec<u8>>>>) {
//         let mut buf = [0u8; PACKET_DATA_SIZE];
//         loop {
//             listen_socket.recv(&mut buf).unwrap();
//             received_packets.lock().unwrap().push(Vec::from(buf));
//         }
//     }

//     #[test]
//     fn test_2shreds_3destinations() {
//         let packet_batch = PacketBatch::new(vec![
//             Packet::new(
//                 [1; PACKET_DATA_SIZE],
//                 Meta {
//                     size: PACKET_DATA_SIZE,
//                     addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
//                     port: 48289, // received on random port
//                     flags: PacketFlags::empty(),
//                 },
//             ),
//             Packet::new(
//                 [2; PACKET_DATA_SIZE],
//                 Meta {
//                     size: PACKET_DATA_SIZE,
//                     addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
//                     port: 9999,
//                     flags: PacketFlags::empty(),
//                 },
//             ),
//         ]);
//         let (packet_sender, packet_receiver) = crossbeam_channel::unbounded::<PacketBatch>();
//         packet_sender.send(packet_batch).unwrap();

//         let dest_socketaddrs = vec![
//             SocketAddr::from_str("0.0.0.0:32881").unwrap(),
//             SocketAddr::from_str("0.0.0.0:33881").unwrap(),
//             SocketAddr::from_str("0.0.0.0:34881").unwrap(),
//         ];

//         let test_listeners = dest_socketaddrs
//             .iter()
//             .map(|socketaddr| {
//                 (
//                     UdpSocket::bind(socketaddr).unwrap(),
//                     *socketaddr,
//                     // store results in vec of packet, where packet is Vec<u8>
//                     Arc::new(Mutex::new(vec![])),
//                 )
//             })
//             .collect::<Vec<_>>();

//         let udp_sender = UdpSocket::bind("0.0.0.0:10000").unwrap();

//         // spawn listeners
//         test_listeners
//             .iter()
//             .for_each(|(listen_socket, _socketaddr, to_receive)| {
//                 let socket = listen_socket.try_clone().unwrap();
//                 let to_receive = to_receive.to_owned();
//                 thread::spawn(move || listen_and_collect(socket, to_receive));
//             });

//         // send packets
//         recv_from_channel_and_send_multiple_dest(
//             packet_receiver.recv(),
//             &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
//                 &mut rand::thread_rng(),
//                 crate::forwarder::DEDUPER_NUM_BITS,
//             ))),
//             &udp_sender,
//             &Arc::new(dest_socketaddrs),
//             accept_all,
//             true,
//             &Arc::new(ShredMetrics::default()),
//         )
//         .unwrap();

//         // allow packets to be received
//         sleep(Duration::from_millis(500));

//         let received = test_listeners
//             .iter()
//             .map(|(_, _, results)| results.clone())
//             .collect::<Vec<_>>();

//         // check results
//         for received in received.iter() {
//             let received = received.lock().unwrap();
//             assert_eq!(received.len(), 2);
//             assert!(received
//                 .iter()
//                 .all(|packet| packet.len() == PACKET_DATA_SIZE));
//             assert_eq!(received[0], [1; PACKET_DATA_SIZE]);
//             assert_eq!(received[1], [2; PACKET_DATA_SIZE]);
//         }

//         assert_eq!(
//             received
//                 .iter()
//                 .fold(0, |acc, elem| acc + elem.lock().unwrap().len()),
//             6
//         );
//     }

//     #[test]
//     fn test_dest_filter() {
//         let packet_batch = PacketBatch::new(vec![
//             Packet::new(
//                 [1; PACKET_DATA_SIZE],
//                 Meta {
//                     size: PACKET_DATA_SIZE,
//                     addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
//                     port: 48289, // received on random port
//                     flags: PacketFlags::empty(),
//                 },
//             ),
//             Packet::new(
//                 [2; PACKET_DATA_SIZE],
//                 Meta {
//                     size: PACKET_DATA_SIZE,
//                     addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
//                     port: 9999,
//                     flags: PacketFlags::empty(),
//                 },
//             ),
//         ]);
//         let (packet_sender, packet_receiver) = crossbeam_channel::unbounded::<PacketBatch>();
//         packet_sender.send(packet_batch).unwrap();

//         let dest_socketaddrs = vec![
//             SocketAddr::from_str("0.0.0.0:32881").unwrap(),
//             SocketAddr::from_str("0.0.0.0:33881").unwrap(),
//             SocketAddr::from_str("0.0.0.0:34881").unwrap(),
//         ];

//         let blacklisted = SocketAddr::from_str("0.0.0.0:34881").unwrap(); // none blacklisted

//         let test_listeners = dest_socketaddrs
//             .iter()
//             .map(|socketaddr| {
//                 (
//                     UdpSocket::bind(socketaddr).unwrap(),
//                     *socketaddr,
//                     // store results in vec of packet, where packet is Vec<u8>
//                     Arc::new(Mutex::new(vec![])),
//                 )
//             })
//             .collect::<Vec<_>>();

//         let udp_sender = UdpSocket::bind("0.0.0.0:10000").unwrap();

//         // spawn listeners
//         test_listeners
//             .iter()
//             .for_each(|(listen_socket, _socketaddr, to_receive)| {
//                 let socket = listen_socket.try_clone().unwrap();
//                 let to_receive = to_receive.to_owned();
//                 thread::spawn(move || listen_and_collect(socket, to_receive));
//             });

//         // send packets
//         recv_from_channel_and_send_multiple_dest(
//             packet_receiver.recv(),
//             &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
//                 &mut rand::thread_rng(),
//                 crate::forwarder::DEDUPER_NUM_BITS,
//             ))),
//             &udp_sender,
//             &Arc::new(dest_socketaddrs),
//             move |_origin, dest: SocketAddr| dest != blacklisted,
//             true,
//             &Arc::new(ShredMetrics::default()),
//         )
//         .unwrap();

//         // allow packets to be received
//         sleep(Duration::from_millis(500));

//         let received = test_listeners
//             .iter()
//             .take(test_listeners.len() - 1) // ignore blacklisted
//             .map(|(_, _, results)| results.clone())
//             .collect::<Vec<_>>();

//         // check results
//         for received in received.iter() {
//             let received = received.lock().unwrap();
//             assert_eq!(received.len(), 2);
//             assert!(received
//                 .iter()
//                 .all(|packet| packet.len() == PACKET_DATA_SIZE));
//             assert_eq!(received[0], [1; PACKET_DATA_SIZE]);
//             assert_eq!(received[1], [2; PACKET_DATA_SIZE]);
//         }

//         {
//             let received = test_listeners[2].2.lock().unwrap(); // ensure blacklisted received nothing
//             assert_eq!(received.len(), 0);
//         }
//         assert_eq!(
//             received
//                 .iter()
//                 .fold(0, |acc, elem| acc + elem.lock().unwrap().len()),
//             4
//         );
//     }
// }

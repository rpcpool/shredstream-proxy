use std::{
    collections::VecDeque, net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket}, num::NonZeroUsize, os::fd::AsRawFd, str::FromStr, sync::{
        Arc, Mutex, atomic::{AtomicBool, Ordering}
    }, thread::JoinHandle, time::{Duration, Instant}
};

use arc_swap::ArcSwap;
use bytes::Buf;
use crossbeam_channel::{Receiver, Sender};
use itertools::izip;
use libc;
use log::{debug, error, info, warn};
use mio::Waker;
use solana_net_utils::SocketConfig;
use solana_perf::deduper::Deduper;
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer::{StreamerReceiveStats},
};

use crate::{
    forwarder::{ShredMetrics, try_create_ipv6_socket}, mem::{FrameBuf, FrameDesc, Rx, SharedMem, Tx}, prom::{
        inc_packets_deduped, inc_packets_forward_failed, observe_dedup_time, observe_recv_interval, observe_send_duration, observe_send_packet_count
    }, recv_mmsg::{PacketRoutingStrategy, TritonPacket}, multicast_config::TritonMulticastConfig
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
    pkt_recv_socket_vec: Vec<UdpSocket>,
    exit: Arc<AtomicBool>,
    forwarder_stats: Arc<StreamerReceiveStats>,
    mut fill_rx: Rx<FrameDesc>,
    packet_tx_vec: Vec<Tx<TritonPacket>>,
    wake_slot: Arc<Mutex<Option<Arc<Waker>>>>,
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
                pkt_recv_socket_vec,
                &exit,
                &forwarder_stats,
                &mut fill_rx,
                &packet_tx_vec,
                wake_slot,
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

            let mut last_batch_to_send = Instant::now();

            assert_eq!(fill_tx_vec.len(), shmem_info_vec.len());

            let mut next_deduper_reset_attempt = Instant::now() + Duration::from_secs(2);
            let mut recycled_frames: Vec<FrameDesc> = Vec::new();

            while !exit.load(Ordering::Relaxed) {
                if next_deduper_reset_attempt.elapsed() > Duration::ZERO {
                    deduper.maybe_reset(
                        &mut rand::thread_rng(),
                        DEDUPER_FALSE_POSITIVE_RATE,
                        DEDUPER_RESET_CYCLE,
                    );
                    next_deduper_reset_attempt = Instant::now() + Duration::from_secs(2);
                    log::debug!(
                        "send_batch_count: {}, duplicate: {}, total-pkt-sent: {}, queue-len: {}, to-recycle: {}",
                        stats.send_batch_count.load(Ordering::Relaxed),
                        stats.duplicate.load(Ordering::Relaxed),
                        stats.send_batch_size_sum.load(Ordering::Relaxed),
                        queued.len(),
                        recycled_frames.len(),
                    );
                }

                // Drain packet_rx as fast as possible until queued is full or no packet is available.
                while queued.len() < UIO_MAXIOV {
                    let Some(packet) = packet_rx.try_recv() else {
                        break;
                    };

                    let data_size = packet.meta.size;
                    let data_slice = &packet.buffer.chunk()[..data_size];
                    let t = Instant::now();
                    if deduper.dedup(data_slice) {
                        let desc = packet.buffer.into_inner();
                        recycled_frames.push(desc);
                        stats.duplicate.fetch_add(1, Ordering::Relaxed);
                        inc_packets_deduped(1);
                    } else {
                        queued.push_back(packet);
                    }
                    let dedup_duration = t.elapsed();
                    observe_dedup_time(dedup_duration.as_micros() as f64);
                }

                let dests = hot_dest_vec.load();
                let dests_len = dests.len();
                assert!(
                    dests_len <= UIO_MAXIOV,
                    "number of destinations ({}) cannot be greater than UIO_MAXIOV ({})",
                    dests_len,
                    UIO_MAXIOV
                );

                // Send as much as possible from queued.
                while !queued.is_empty() {
                    next_batch_send.clear();

                    while next_batch_send.len() < UIO_MAXIOV && !queued.is_empty() {
                        let remaining = UIO_MAXIOV - next_batch_send.len();
                        if dests_len > remaining {
                            break;
                        }

                        let Some(packet) = queued.pop_front() else {
                            break;
                        };
                        let buf = packet.buffer;
                        let desc = unsafe { buf.detach_desc() };
                        recycled_frames.push(desc);

                        for dest in dests.iter() {
                            let buf_clone =
                                unsafe { buf.unsafe_subslice_clone(0, packet.meta.size) };
                            next_batch_send.push((buf_clone, *dest));
                        }
                    }

                    if next_batch_send.is_empty() {
                        break;
                    }

                    let batch_send_ts = Instant::now();
                    let e = last_batch_to_send.elapsed();
                    last_batch_to_send = Instant::now();

                    observe_recv_interval(e.as_micros() as f64);
                    match batch_send(&send_socket, &next_batch_send) {
                        Ok(_) => {
                            let send_duration = batch_send_ts.elapsed();
                            stats
                                .batch_send_time_spent
                                .fetch_add(send_duration.as_micros() as u64, Ordering::Relaxed);
                            stats.send_batch_count.fetch_add(1, Ordering::Relaxed);
                            stats
                                .send_batch_size_sum
                                .fetch_add(next_batch_send.len() as u64, Ordering::Relaxed);
                            observe_send_duration(send_duration.as_micros() as f64);
                            observe_send_packet_count(next_batch_send.len() as f64);
                        }
                        Err(SendPktsError::IoError(err, num_failed)) => {
                            error!(
                                "Failed to send batch of size {}. {num_failed} packets failed. Error: {err}",
                                next_batch_send.len()
                            );
                            inc_packets_forward_failed(num_failed as u64);
                        }
                    }
                }

                // Recycle all used frames.
                while let Some(desc) = recycled_frames.pop() {
                    fill_tx_vec[desc.shmem_idx]
                        .send(desc)
                        .expect("frame recycling");
                }

                if queued.is_empty() && next_batch_send.is_empty() && recycled_frames.is_empty() {
                    std::thread::yield_now();
                }
            }
            log::info!("Exiting pkt_fwd_tile {}", packet_fwd_idx);
            drop(tile_drop_sig);
        })
}

#[allow(clippy::too_many_arguments)]
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
    doublezero_sk_vec: Vec<UdpSocket>,
) where
    R: PacketRoutingStrategy + Send + Sync + 'static,
{
    assert!(num_pkt_recv_tiles > 0, "num_pkt_recv_tiles must be > 0");
    assert!(num_pkt_fwd_tiles > 0, "num_pkt_fwd_tiles must be > 0");
    let mut tile_thread_vec: Vec<JoinHandle<()>> = Vec::new();
    // Build pkt_recv sockets
    let pkt_recv_multicast_sk_vec = if let Some(multicast_config) = multticast_config {
        log::info!("Using Triton multicast configuration for pkt_recv tiles");
        let vec = crate::multicast_config::create_multicast_sockets_triton(
            &multicast_config, 
        ).expect("multicast-config");
        Some(vec![vec])
    } else {
        None
    };

    assert!(doublezero_sk_vec.len() <= num_pkt_recv_tiles, "doublezero_v4_sk_vec.len() ({}) > num_pkt_recv_tiles ({})", doublezero_sk_vec.len(), num_pkt_recv_tiles);

    let (_port, pkt_recv_sk_vec) = solana_net_utils::multi_bind_in_range_with_config(
        src_ip,
        (src_port, src_port + 1),
        SocketConfig::default().reuseport(true),
        num_pkt_recv_tiles,
    )
    .unwrap_or_else(|_| {
        panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
    });
    assert!(pkt_recv_sk_vec.len() == num_pkt_recv_tiles, "pkt_recv_sk_vec.len() ({}) != num_pkt_recv_tiles ({})", pkt_recv_sk_vec.len(), num_pkt_recv_tiles);

    if let Some(multicast_sk_vec) = &pkt_recv_multicast_sk_vec {
        assert!(multicast_sk_vec.len() == num_pkt_recv_tiles, "multicast_sk_vec.len() ({}) != num_pkt_recv_tiles ({})", multicast_sk_vec.len(), num_pkt_recv_tiles);
    }

    // Make sure socket are set to nonblocking
    for sk in &pkt_recv_sk_vec {
        sk.set_nonblocking(true).expect("pkt_recv_sk nonblocking");
    }


    let mut pkt_recv_sk_raw_fd_vec: Vec<i32> = Vec::with_capacity(num_pkt_recv_tiles);
    for sk in &pkt_recv_sk_vec {
        pkt_recv_sk_raw_fd_vec.push(sk.as_raw_fd());
    }

    let num_frames =
        pkt_recv_tile_mem_config.memory_size as usize / pkt_recv_tile_mem_config.frame_size;
    let frame_size = pkt_recv_tile_mem_config.frame_size;

    let tile_wait_group = TileWaitGroup::new();
    let mut shmem_info_vec: Vec<SharedMemInfo> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut fill_tx_vec: Vec<Tx<FrameDesc>> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut fill_rx_vec: Vec<Rx<FrameDesc>> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut shmem_vec: Vec<SharedMem> = Vec::with_capacity(num_pkt_recv_tiles);

    let mut pkt_fwd_sk_vec: Vec<UdpSocket> = Vec::with_capacity(num_pkt_fwd_tiles);
    let mut pkt_fwd_sk_raw_fd_vec: Vec<i32> = Vec::with_capacity(num_pkt_fwd_tiles);

    let mut packet_rx_vec: Vec<Rx<TritonPacket>> = Vec::with_capacity(num_pkt_fwd_tiles);
    let mut packet_tx_vec: Vec<Tx<TritonPacket>> = Vec::with_capacity(num_pkt_fwd_tiles);
    let mut recv_wake_slots: Vec<Arc<Mutex<Option<Arc<Waker>>>>> =
        Vec::with_capacity(num_pkt_recv_tiles);

    // Create the shared memory regions for recv tiles
    for shmem_idx in 0..num_pkt_recv_tiles {
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
                shmem_idx,
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
        pkt_fwd_sk_raw_fd_vec.push(send_socket.as_raw_fd());
        pkt_fwd_sk_vec.push(send_socket);
    }


    let pkt_fwd_tile_ring_capacity = num_frames * num_pkt_recv_tiles;
    log::info!(
        "Setting pkt_fwd tile's message ring capacity to {} (num_frames {} * num_pkt_recv_tiles {})",
        pkt_fwd_tile_ring_capacity,
        num_frames,
        num_pkt_recv_tiles
    );

    // Create pkt_fwd message rings
    // One ring per pkt_fwd tile
    for _ in 0..num_pkt_fwd_tiles {
        // Worst case scenario all frames from all pkt_recv tiles are sent to this pkt_fwd tile
        // We set the ring capacity to that
        let (packet_tx, packet_rx) = crate::mem::message_ring(pkt_fwd_tile_ring_capacity).expect("pkt_fwd ring");
        packet_tx_vec.push(packet_tx);
        packet_rx_vec.push(packet_rx);
    }

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

        let mut recv_pkt_vec = vec![
            pkt_recv_sk
        ];

        if let Some(multicast_sk_vec) = &pkt_recv_multicast_sk_vec {
            recv_pkt_vec.push(multicast_sk_vec[pkt_recv_idx].try_clone().expect("multicast sk clone"));
        }

        if let Some(doublezero_v4_sk) = doublezero_sk_vec.get(pkt_recv_idx) {
            recv_pkt_vec.push(doublezero_v4_sk.try_clone().expect("doublezero v4 sk clone"));
        }
        
        let exit = Arc::clone(&exit);
        let forwarder_stats = Arc::clone(&pk_recv_stats);
        let packet_tx_vec_clone = packet_tx_vec.clone();
        let wake_slot: Arc<Mutex<Option<Arc<Waker>>>> = Arc::new(Mutex::new(None));
        recv_wake_slots.push(Arc::clone(&wake_slot));
        let pkt_router_clone = pkt_router.clone();
        let jh = packet_recv_tile(
            pkt_recv_idx,
            recv_pkt_vec,
            exit,
            forwarder_stats,
            fill_rx,
            packet_tx_vec_clone,
            wake_slot,
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
    for wake_slot in &recv_wake_slots {
        if let Some(waker) = wake_slot
            .lock()
            .expect("recv wake slot lock poisoned")
            .as_ref()
            .cloned()
        {
            let _ = waker.wake();
        }
    }
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

/// Reset dedup + send metrics to influx
pub fn start_forwarder_accessory_thread(
    metrics: Arc<ShredMetrics>,
    metrics_update_interval_ms: u64,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("ssPxyAccessory".to_string())
        .spawn(move || {
            let metrics_tick =
                crossbeam_channel::tick(Duration::from_millis(metrics_update_interval_ms));
            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    // send metrics to influx
                    recv(metrics_tick) -> _ => {
                        metrics.report();
                        metrics.reset();
                    }

                    // handle SIGINT shutdown
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use solana_sdk::packet::PACKET_DATA_SIZE;
    use std::net::UdpSocket;

    #[test]
    fn test_pkt_recv_mem_sizing_from_str_aliases() {
        assert!(matches!(
            PktRecvMemSizing::from_str("xs"),
            Ok(PktRecvMemSizing::XSmall)
        ));
        assert!(matches!(
            PktRecvMemSizing::from_str("medium"),
            Ok(PktRecvMemSizing::Medium)
        ));
        assert!(matches!(
            PktRecvMemSizing::from_str("2xl"),
            Ok(PktRecvMemSizing::XXLarge)
        ));
        assert!(matches!(
            PktRecvMemSizing::from_str("5XL"),
            Ok(PktRecvMemSizing::XXXXXLarge)
        ));
    }

    #[test]
    fn test_pkt_recv_mem_sizing_from_str_invalid() {
        let invalid = "huge";
        let err = PktRecvMemSizing::from_str(invalid).unwrap_err();
        assert_eq!(err, invalid);
    }

    #[test]
    fn test_pkt_recv_tile_mem_config_default() {
        let cfg = PktRecvTileMemConfig::default();
        assert_eq!(cfg.frame_size, 2048);
        assert!(matches!(cfg.memory_size, PktRecvMemSizing::XSmall));
        assert!(!cfg.hugepage);
    }

    #[test]
    fn test_tile_wait_group_wait_first_reports_drop() {
        let wait_group = TileWaitGroup::new();
        let sig = wait_group.get_tile_closed_signal(TileKind::PktFwd, 7);
        drop(sig);
        let (kind, idx) = wait_group.wait_first();
        assert_eq!(kind, TileKind::PktFwd);
        assert_eq!(idx, 7);
    }

    #[test]
    fn test_packet_fwd_tile_sends_and_recycles_frame() {
        let frame_size = 2048usize;
        let listener = UdpSocket::bind("127.0.0.1:0").expect("listener bind");
        listener
            .set_read_timeout(Some(Duration::from_millis(1000)))
            .expect("listener set_read_timeout");
        let listener_addr = listener.local_addr().expect("listener local_addr");

        let send_socket = UdpSocket::bind("0.0.0.0:0").expect("send bind");
        let hot_dest_vec = Arc::new(ArcSwap::from_pointee(vec![listener_addr]));

        let shmem = SharedMem::new(frame_size, 1, false).expect("shmem");
        let frame_desc = FrameDesc {
            ptr: shmem.ptr,
            frame_size,
            shmem_idx: 0,
        };

        let mut frame_bufmut = frame_desc.as_mut_buf();
        let payload = b"hello-forwarder";
        frame_bufmut.put_slice(payload);
        let frame_buf: FrameBuf = frame_bufmut.into();

        let mut packet = TritonPacket::new(frame_buf);
        packet.meta_mut().size = payload.len();
        // Use a non-local origin so the destination filter doesn't skip forwarding.
        packet
            .meta_mut()
            .set_socket_addr(&SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 1, 1)), 12345));

        let (fill_tx, mut fill_rx) = crate::mem::message_ring::<FrameDesc>(8).expect("fill ring");
        let (packet_tx, packet_rx) =
            crate::mem::message_ring::<TritonPacket>(8).expect("packet ring");

        let shmem_info_vec = vec![SharedMemInfo {
            start_ptr: shmem.ptr,
            len: shmem.len(),
        }];
        let fill_tx_vec = vec![fill_tx];

        let wait_group = TileWaitGroup::new();
        let exit = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ShredMetrics::default());
        let jh = packet_fwd_tile(
            0,
            hot_dest_vec,
            send_socket,
            packet_rx,
            fill_tx_vec,
            shmem_info_vec,
            stats,
            Arc::clone(&exit),
            wait_group.get_tile_closed_signal(TileKind::PktFwd, 0),
        )
        .expect("spawn packet_fwd_tile");

        packet_tx.send(packet).expect("send packet to fwd tile");

        let mut recv_buf = [0u8; PACKET_DATA_SIZE];
        let (n, _) = listener.recv_from(&mut recv_buf).expect("recv forwarded packet");
        assert_eq!(&recv_buf[..n], payload);

        let recycled = fill_rx
            .recv_timeout(Duration::from_millis(1000))
            .expect("recycled frame");
        assert_eq!(recycled.ptr, shmem.ptr);
        assert_eq!(recycled.frame_size, frame_size);

        exit.store(true, Ordering::Release);
        drop(packet_tx);
        let _ = wait_group.wait_first();
        jh.join().expect("join packet_fwd_tile");
    }
}

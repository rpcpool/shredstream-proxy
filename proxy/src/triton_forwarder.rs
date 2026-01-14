use std::{
    collections::{HashSet, VecDeque}, net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket}, str::FromStr, sync::{
        Arc, RwLock, atomic::{AtomicBool, AtomicU64, Ordering}
    }, thread::{Builder, JoinHandle}, time::{Duration, Instant, SystemTime}
};

use arc_swap::ArcSwap;
use bytes::Buf;
use crossbeam_channel::{Receiver, RecvError};
use dashmap::DashMap;
use itertools::{Itertools, izip};
use jito_protos::shredstream::{Entry as PbEntry, TraceShred};
use log::{debug, error, info, warn};
use libc;
use prost::Message;
use socket2::{Domain, Protocol, Socket, Type};
use solana_client::client_error::reqwest;
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_net_utils::SocketConfig;
use solana_perf::{
    deduper::Deduper,
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer::{self, StreamerReceiveStats},
};
use tokio::sync::broadcast::Sender;

use crate::{
    ShredstreamProxyError, forwarder::{ShredMetrics, try_create_ipv6_socket}, mem::{FrameBuf, FrameDesc, Rx, SharedMem, Tx}, prom::{
        inc_packets_by_source, inc_packets_deduped, inc_packets_forward_failed, inc_packets_forwarded, inc_packets_received, observe_dedup_time, observe_recv_interval, observe_recv_packet_count, observe_send_duration, observe_send_packet_count
    }, recv_mmsg::{PacketRoutingStrategy, TritonPacket}, resolve_hostname_port
};

// values copied from https://github.com/solana-labs/solana/blob/33bde55bbdde13003acf45bb6afe6db4ab599ae4/core/src/sigverify_shreds.rs#L20
pub const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
pub const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
pub const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);
pub const IP_MULTICAST_TTL: u32 = 8;


#[derive(Debug, Clone, Copy, Default)]
pub enum ReceiverMemorySizing {
    #[default]
    XSmall = 134217728, // 128MiB
    Small = 268435456, // 256MiB
    Medium = 536870912, // 512MiB
    Large = 1073741824, // 1GiB
    XLarge = 2147483648, // 2GiB
    XXLarge = 4294967296, // 4GiB
    XXXLarge = 8589934592, // 8GiB
    XXXXLarge = 17179869184, // 16GiB
    XXXXXLarge = 34359738368, // 32GiB
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid ReceiverMemoryCapacity: {0}")]
pub struct ReceiverMemoryCapacityFromStrErr(String);


impl FromStr for ReceiverMemorySizing {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "xsmall" | "xs" => Ok(ReceiverMemorySizing::XSmall),
            "small" | "s" => Ok(ReceiverMemorySizing::Small),
            "medium" | "m" => Ok(ReceiverMemorySizing::Medium),
            "large" | "l" => Ok(ReceiverMemorySizing::Large),
            "xlarge" | "xl" => Ok(ReceiverMemorySizing::XLarge),
            "xxlarge" | "xxl" | "2xl" => Ok(ReceiverMemorySizing::XXLarge),
            "xxxlarge" | "xxxl" | "3xl" => Ok(ReceiverMemorySizing::XXXLarge),
            "xxxxlarge" | "xxxxl" | "4xl"  => Ok(ReceiverMemorySizing::XXXXLarge),
            "xxxxxlarge" | "xxxxxl" | "5xl" => Ok(ReceiverMemorySizing::XXXXXLarge),
            _ => Err(s.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PacketRecvTileMemConfig {
    pub frame_size: usize,
    pub memory_size: ReceiverMemorySizing,
    pub hugepage: bool,

}

impl Default for PacketRecvTileMemConfig {
    fn default() -> Self {
        Self {
            frame_size: 2048,
            memory_size: ReceiverMemorySizing::default(),
            hugepage: false,
        }
    }
}

fn packet_recv_tile<R>(
    sockets: Vec<UdpSocket>,
    src_addr: IpAddr,
    src_port: u16,
    exit: Arc<AtomicBool>,
    forwarder_stats: Arc<StreamerReceiveStats>,
    fill_rx_vec: Vec<Rx<FrameDesc>>,
    packet_tx_vec: Vec<Tx<TritonPacket>>,
    packet_router: R,
    threads: &mut Vec<JoinHandle<()>>,
) -> std::io::Result<()>
    where R: PacketRoutingStrategy + Send + 'static,
{ 
    assert!(sockets.len() == fill_rx_vec.len(), "mismatched fill_rx_vec and sockets length");
    assert!(sockets.len() == packet_tx_vec.len(), "mismatched packet_tx_vec and sockets length");

    // let (_port, sockets) = solana_net_utils::multi_bind_in_range_with_config(
    //     src_addr,
    //     (src_port, src_port + 1),
    //     SocketConfig::default().reuseport(true),
    //     num_receiver,
    // )
    // .unwrap_or_else(|_| {
    //     panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
    // });

    for (thread_id, socket, mut fill_rx) in izip!(0..sockets.len(), sockets.into_iter(), fill_rx_vec.into_iter()) {

        // let shmem = SharedMem::new(
        //     mem_config.frame_size,
        //     mem_config.memory_size as usize / mem_config.frame_size,
        //     mem_config.hugepage,
        // ).expect("SharedMem::new");

        let socket = Arc::new(socket);
        let exit = Arc::clone(&exit);
        let stats = Arc::clone(&forwarder_stats);
        let packet_tx_vec = packet_tx_vec.clone();
        let packet_router = R::clone(&packet_router);
        let th = std::thread::Builder::new()
            .name(format!("ssListen{thread_id}"))
            .spawn(move || {
                let socket = socket;
                crate::recv_mmsg::recv_loop(
                    &socket,
                    &exit,
                    &stats,
                    Duration::default(),
                    &mut fill_rx,
                    &packet_tx_vec,
                    packet_router,
                )
                .expect("recv_loop")
            })?;

        threads.push(th);
    }
    Ok(())
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct SharedMemInfo {
    pub start_ptr: *const u8,
    pub len: usize, // always a power of 2
}

unsafe impl Send for SharedMemInfo {}
unsafe impl Sync for SharedMemInfo {}

fn packet_fwd_tile(
    packet_fwd_idx: usize,
    hot_dest_vec: Arc<ArcSwap<Vec<SocketAddr>>>,
    send_socket: UdpSocket,
    mut packet_rx: Rx<TritonPacket>,
    fill_tx_vec: Vec<Tx<FrameDesc>>,
    shmem_info_vec: Vec<SharedMemInfo>,
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
                assert!(shmem_info.len.is_power_of_two(), "shmem_info.len must be a power of 2");
            }

            let mut next_deduper_reset_attempt = Instant::now() + Duration::from_secs(2);
            let mut recycled_frames: Vec<FrameDesc> = Vec::with_capacity(UIO_MAXIOV);
            loop {

                if next_deduper_reset_attempt.elapsed() > Duration::ZERO {
                    deduper.maybe_reset(&mut rand::thread_rng(), DEDUPER_FALSE_POSITIVE_RATE, DEDUPER_RESET_CYCLE);
                    next_deduper_reset_attempt = Instant::now() + Duration::from_secs(2);
                }

                // Fill up the queued OR recycled_frames as much as possible
                while queued.len() < UIO_MAXIOV && recycled_frames.len() < UIO_MAXIOV {
                    // Fill the batch as much as possible.
                    let packet = packet_rx.recv();
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

                let dests = hot_dest_vec.load();
                let dests_len = dests.len();

                // Fill up the next_batch_send
                'fill_batch_send: while next_batch_send.len() < UIO_MAXIOV && queued.len() > 0 && dests_len > 0 {

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

                assert!(next_batch_send.len() <= UIO_MAXIOV, "next_batch_send.len() = {}", next_batch_send.len());
                assert!(recycled_frames.len() <= UIO_MAXIOV, "recycled_frames.len() = {}", recycled_frames.len());
                assert!(queued.len() <= UIO_MAXIOV, "queued.len() = {}", queued.len());

                match batch_send(&send_socket, &next_batch_send) {
                    Ok(_) => {
                        // Successfully sent all packets in the batch
                    }
                    Err(SendPktsError::IoError(err, num_failed)) => {
                        error!(
                            "Failed to send batch of size {}. \
                             {num_failed} packets failed. Error: {err}",
                            next_batch_send.len()
                        );
                    }
                }


                // Recycle all used frames
                while let Some(desc) = recycled_frames.pop() {
                    let fill_ring_idx = shmem_info_vec.iter().find_position(|shmem_info| {
                        (desc.ptr as usize) & (shmem_info.len - 1) == (shmem_info.start_ptr as usize)
                    }).expect("unknown frame desc").0;
                    fill_tx_vec[fill_ring_idx].send(desc).expect("frame recycling");
                }

            }
        })
}

#[derive(thiserror::Error, Debug)]
pub enum ProxySystemError {
    #[error(transparent)]
    IoError(std::io::Error),
    #[error(transparent)]
    AllocError(crate::mem::AllocError),
}


pub fn spawn_proxy_system<R>(
    pkt_recv_tile_mem_config: PacketRecvTileMemConfig,
    dest_addr_vec: Arc<ArcSwap<Vec<SocketAddr>>>,
    src_ip: IpAddr,
    src_port: u16,
    num_pkt_recv_tiles: usize,
    num_pkt_fwd_tiles: usize,
    pkt_router: R,
    exit: Arc<AtomicBool>,
    stats: Arc<StreamerReceiveStats>,
) -> JoinHandle<()>
    where R: PacketRoutingStrategy + Send + Sync + 'static,
{

    // Build pkt_recv sockets
    let (_port, sockets) = solana_net_utils::multi_bind_in_range_with_config(
        src_ip,
        (src_port, src_port + 1),
        SocketConfig::default().reuseport(true),
        num_pkt_recv_tiles,
    )
    .unwrap_or_else(|_| {
        panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
    });


    // Create the shared memory regions for recv tiles
    let mut shmem_info_vec: Vec<SharedMemInfo> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut fill_tx_vec: Vec<Tx<FrameDesc>> = Vec::with_capacity(num_pkt_recv_tiles);
    let mut fill_rx_vec: Vec<Rx<FrameDesc>> = Vec::with_capacity(num_pkt_recv_tiles);
    letm
    let mut shmem_vec: Vec<SharedMem> = Vec::with_capacity(num_pkt_recv_tiles);
    for _ in 0..num_pkt_recv_tiles {
        let frame_size = pkt_recv_tile_mem_config.frame_size;
        let num_frames = pkt_recv_tile_mem_config.memory_size as usize / pkt_recv_tile_mem_config.frame_size;
        assert!(num_frames.is_power_of_two(), "num_frames must be a power of 2");
        assert!(frame_size.is_power_of_two(), "frame_size must be a power of 2");
        let shmem = SharedMem::new(
            frame_size,
            num_frames,
            pkt_recv_tile_mem_config.hugepage,
        ).expect("SharedMem::new");

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
            fill_tx.send(frame_desc).expect("initial frame ring population");
        }
        shmem_vec.push(shmem);
        fill_tx_vec.push(fill_tx);
        fill_rx_vec.push(fill_rx);
    } 

    // Create socket for sending packets


    let send_socket = {
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        match try_create_ipv6_socket(ipv6_addr) {
            Ok(socket) => {
                info!("Successfully bound send socket to IPv6 dual-stack address.");
                socket.set_multicast_loop_v6(false)
                    .expect("Failed to disable IPv6 multicast loopback");
                socket
            }
            Err(e) if e.raw_os_error() == Some(libc::EAFNOSUPPORT) => {
                // This error (code 97 on Linux) means IPv6 is not supported.
                warn!("IPv6 not available. Falling back to IPv4-only for sending.");
                let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
                let socket = UdpSocket::bind(ipv4_addr)
                    .expect("Failed to bind to IPv4 socket after IPv6 failed");
                socket.set_multicast_ttl_v4(IP_MULTICAST_TTL).expect("IP_MULTICAST_TTL_V4");
                socket.set_multicast_loop_v4(false)
                    .expect("Failed to disable IPv4 multicast loopback");
                socket
            }
            Err(e) => {
                // For any other error (e.g., port in use), panic.
                panic!("Failed to bind send socket with an unexpected error: {e}");
            }
        }
    };


    


    todo!()
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

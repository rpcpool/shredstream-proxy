use std::{
    collections::HashMap, io::{self, Error, ErrorKind}, net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs}, num::NonZeroUsize, panic, path::{Path, PathBuf}, str::FromStr, sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }, thread::{self, sleep, spawn, JoinHandle}, time::Duration
};

use arc_swap::ArcSwap;
use clap::{arg, Parser};
use crossbeam_channel::{Receiver, RecvError, Sender};
use log::*;
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_client::client_error::{reqwest, ClientError};
use solana_ledger::shred::Shred;
use solana_metrics::set_host_id;
use solana_sdk::{clock::Slot, signature::read_keypair_file};
use solana_streamer::streamer::StreamerReceiveStats;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

use crate::{
    forwarder::ShredMetrics, triton_multicast_config::{TritonMulticastConfig, TritonMulticastConfigV4, TritonMulticastConfigV6, create_multicast_sockets_triton}, recv_mmsg::FECSetRoutingStrategy, token_authenticator::BlockEngineConnectionError, triton_forwarder::PktRecvTileMemConfig,
};
pub mod deshred;
pub mod forwarder;
pub mod heartbeat;
pub mod triton_multicast_config;
pub mod server;
pub mod token_authenticator;
pub mod prom;
pub mod recv_mmsg;
pub mod mem;
pub mod triton_forwarder;

use triton_forwarder::{PktRecvMemSizing};

#[derive(Clone, Debug, Parser)]
#[clap(author, version, about, long_about = None)]
// https://docs.rs/clap/latest/clap/_derive/_cookbook/git_derive/index.html
struct Args {
    #[command(subcommand)]
    shredstream_args: ProxySubcommands,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum ProxySubcommands {
    /// Requests shreds from Jito and sends to all destinations.
    Shredstream(ShredstreamArgs),

    /// Does not request shreds from Jito. Sends anything received on `src-bind-addr`:`src-bind-port` to all destinations.
    ForwardOnly(CommonArgs),
}

#[derive(clap::Args, Clone, Debug)]
struct ShredstreamArgs {
    /// Address for Jito Block Engine.
    /// See https://jito-labs.gitbook.io/mev/searcher-resources/block-engine#connection-details
    #[arg(long, env)]
    block_engine_url: String,

    /// Manual override for auth service address. For internal use.
    #[arg(long, env)]
    auth_url: Option<String>,

    /// Path to keypair file used to authenticate with the backend.
    #[arg(long, env)]
    auth_keypair: PathBuf,

    /// Desired regions to receive heartbeats from.
    /// Receives `n` different streams. Requires at least 1 region, comma separated.
    #[arg(long, env, value_delimiter = ',', required(true))]
    desired_regions: Vec<String>,

    #[clap(flatten)]
    common_args: CommonArgs,
}

#[derive(clap::Args, Clone, Debug)]
struct CommonArgs {
    /// Address where Shredstream proxy listens.
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    src_bind_addr: IpAddr,

    /// Port where Shredstream proxy listens. Use `0` for random ephemeral port.
    #[arg(long, env, default_value_t = 20_000)]
    src_bind_port: u16,

    /// Static set of IP:Port where Shredstream proxy forwards shreds to, comma separated.
    /// Eg. `127.0.0.1:8001,10.0.0.1:8001`.
    // Note: store the original string, so we can do hostname resolution when refreshing destinations
    #[arg(long, env, value_delimiter = ',', value_parser = resolve_hostname_port)]
    dest_ip_ports: Vec<(SocketAddr, String)>,

    /// Http JSON endpoint to dynamically get IPs for Shredstream proxy to forward shreds.
    /// Endpoints are then set-union with `dest-ip-ports`.
    #[arg(long, env)]
    endpoint_discovery_url: Option<String>,

    /// Port to send shreds to for hosts fetched via `endpoint-discovery-url`.
    /// Port can be found using `scripts/get_tvu_port.sh`.
    /// See https://jito-labs.gitbook.io/mev/searcher-services/shredstream#running-shredstream
    #[arg(long, env)]
    discovered_endpoints_port: Option<u16>,

    /// Interval between logging stats to stdout and influx
    #[arg(long, env, default_value_t = 15_000)]
    metrics_report_interval_ms: u64,

    /// Logs trace shreds to stdout and influx
    #[arg(long, env, default_value_t = false)]
    debug_trace_shred: bool,

    /// Public IP address to use.
    /// Overrides value fetched from `ifconfig.me`.
    #[arg(long, env)]
    public_ip: Option<IpAddr>,

    /// Number of threads to use. Defaults to use up to 4.
    #[arg(long, env)]
    num_threads: Option<usize>,

    ///
    /// The multicast group (ip addr) to join for receiving shreds.
    /// Multicast groups supports IPv4 and IPv6.
    #[arg(long, env)]
    triton_multicast_group: Option<IpAddr>,
    /// The interface to bind to for triton multicast.
    /// If IPV6 is used, this argument must be provided.
    /// If ipv4, then optional (listen on all interfaces if not provided).
    #[arg(long, env)]
    triton_multicast_bind_interface: Option<String>,
    #[arg(long, env, default_value_t = 1)]
    triton_multicast_num_threads: usize,

    /// Address to bind prometheus metrics server to. If not provided, prometheus server is disabled.
    #[arg(long, env)]
    prometheus_bind_addr: Option<SocketAddr>,

    /// Number of tiles dedicated to receiving packets. If not provided, defaults to number of CPU cores is 1.
    #[arg(long, env)]
    num_pkt_recv_tile: Option<NonZeroUsize>,

    /// Number of tiles dedicated to forwarding packets. If not provided, defaults to number of CPU cores is 1.
    #[arg(long, env)]
    num_pkt_fwd_tile: Option<NonZeroUsize>,

    /// Memory sizing for EACH packet receiver, uses t-shirt size convention (xs (default),s,m,l,xl,2xl,3xl,4xl,5xl). Each size increase double the memory, starting at 128MiB for x-small.
    #[arg(long, env)]
    pkt_recv_channel_memsize: Option<PktRecvMemSizing>,
}

#[derive(Debug, Error)]
pub enum ShredstreamProxyError {
    #[error("TonicError {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("GrpcError {0}")]
    GrpcError(#[from] Status),
    #[error("ReqwestError {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("RpcError {0}")]
    RpcError(#[from] ClientError),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("RecvError {0}")]
    RecvError(#[from] RecvError),
    #[error("IoError {0}")]
    IoError(#[from] io::Error),
    #[error("Shutdown")]
    Shutdown,
}

fn resolve_hostname_port(hostname_port: &str) -> io::Result<(SocketAddr, String)> {
    let socketaddr = hostname_port.to_socket_addrs()?.next().ok_or_else(|| {
        Error::new(
            ErrorKind::AddrNotAvailable,
            format!("Could not find destination {hostname_port}"),
        )
    })?;

    Ok((socketaddr, hostname_port.to_string()))
}

/// Returns public-facing IPV4 address
pub fn get_public_ip() -> reqwest::Result<IpAddr> {
    info!("Requesting public ip from ifconfig.me...");
    let client = reqwest::blocking::Client::builder()
        .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .build()?;
    let response = client.get("https://ifconfig.me/ip").send()?.text()?;
    let public_ip = IpAddr::from_str(&response).unwrap();
    info!("Retrieved public ip: {public_ip:?}");

    Ok(public_ip)
}

// Creates a channel that gets a message every time `SIGINT` is signalled.
fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    thread::spawn(move || {
        for _ in signals.forever() {
            exit.store(true, Ordering::SeqCst);
            // send shutdown signal multiple times since crossbeam doesn't have broadcast channels
            // each thread will consume a shutdown signal
            for _ in 0..256 {
                if s_thread.send(()).is_err() {
                    break;
                }
            }
        }
    });

    Ok((s, r))
}

pub type ReconstructedShredsMap = HashMap<Slot, HashMap<u32 /* fec_set_index */, Vec<Shred>>>;
fn main() -> Result<(), ShredstreamProxyError> {
    env_logger::builder().init();
    let prom_registry  = prometheus::Registry::new();
    prom::register_metrics(&prom_registry);
    let all_args: Args = Args::parse();

    let shredstream_args = all_args.shredstream_args.clone();
    // common args
    let args = match all_args.shredstream_args {
        ProxySubcommands::Shredstream(x) => x.common_args,
        ProxySubcommands::ForwardOnly(x) => x,
    };
    set_host_id(hostname::get()?.into_string().unwrap());
    if (args.endpoint_discovery_url.is_none() && args.discovered_endpoints_port.is_some())
        || (args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_none())
    {
        return Err(ShredstreamProxyError::IoError(io::Error::new(ErrorKind::InvalidInput, "Invalid arguments provided, dynamic endpoints requires both --endpoint-discovery-url and --discovered-endpoints-port.")));
    }
    if args.endpoint_discovery_url.is_none()
        && args.discovered_endpoints_port.is_none()
        && args.dest_ip_ports.is_empty()
    {
        return Err(ShredstreamProxyError::IoError(io::Error::new(ErrorKind::InvalidInput, "No destinations found. You must provide values for --dest-ip-ports or --endpoint-discovery-url.")));
    }

    let exit = Arc::new(AtomicBool::new(false));
    let (shutdown_sender, shutdown_receiver) =
        shutdown_notifier(exit.clone()).expect("Failed to set up signal handler");
    let panic_hook = panic::take_hook();
    {
        let exit = exit.clone();
        panic::set_hook(Box::new(move |panic_info| {
            exit.store(true, Ordering::SeqCst);
            let _ = shutdown_sender.send(());
            error!("exiting process");
            sleep(Duration::from_secs(1));
            // invoke the default handler and exit the process
            panic_hook(panic_info);
        }));
    }

    let metrics = Arc::new(ShredMetrics::new(false));
    

    let mut thread_handles = vec![];
    if let ProxySubcommands::Shredstream(args) = shredstream_args {
        let runtime = Runtime::new()?;
        if args.desired_regions.len() > 2 {
            warn!(
                "Too many regions requested, only regions: {:?} will be used",
                &args.desired_regions[..2]
            );
        }
        let heartbeat_hdl =
            start_heartbeat(args, &exit, &shutdown_receiver, runtime, metrics.clone());
        thread_handles.push(heartbeat_hdl);
    }

    // share sockets between refresh and forwarder thread
    let unioned_dest_sockets = Arc::new(ArcSwap::from_pointee(
        args.dest_ip_ports
            .iter()
            .map(|x| x.0)
            .collect::<Vec<SocketAddr>>(),
    ));

    let forward_stats = Arc::new(StreamerReceiveStats::new("shredstream_proxy-listen_thread"));
    let use_discovery_service =
        args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_some();

    let maybe_triton_multicast_config = match args.triton_multicast_group {
        Some(multicast_group) => {
            match multicast_group {
                IpAddr::V4(ipv4) => {
                    Some(TritonMulticastConfig::Ipv4(TritonMulticastConfigV4 {
                        multicast_ip: ipv4,
                        bind_ifname: args.triton_multicast_bind_interface,
                    }))
                }
                IpAddr::V6(ipv6) => {
                    Some(TritonMulticastConfig::Ipv6(TritonMulticastConfigV6 {
                        multicast_ip: ipv6,
                        device_ifname: args.triton_multicast_bind_interface
                            .ok_or_else(|| {
                                io::Error::new(
                                    ErrorKind::InvalidInput,
                                    "triton-multicast-bind-interface is required for IPv6",
                                )
                            })?,
                    }))
                }
            }
        }
        None => None,
    };

    let pkt_recv_tile_mem_config = PktRecvTileMemConfig {
        memory_size: args.pkt_recv_channel_memsize.unwrap_or_default(),
        ..Default::default()
    };
    let proxy_th = {
        let exit = Arc::clone(&exit);
        let pkt_recv_stats = forward_stats.clone();
        let pkt_fwd_stats = metrics.clone();
        let unioned_dest_sockets = Arc::clone(&unioned_dest_sockets);
        std::thread::Builder::new()
        .name("tritonProxyMain".to_string())
        .spawn(move || {
            triton_forwarder::run_proxy_system(
                pkt_recv_tile_mem_config,
                unioned_dest_sockets,
                maybe_triton_multicast_config,
                args.src_bind_addr,
                args.src_bind_port,
                args.num_pkt_recv_tile.map(|x| x.get()).unwrap_or(1),
                args.num_pkt_fwd_tile.map(|x| x.get()).unwrap_or(1),
                FECSetRoutingStrategy,
                exit,
                pkt_recv_stats,
                pkt_fwd_stats,
            );
        })
        .expect("tritonProxyMain")
    };

    thread_handles.push(proxy_th);

    let report_metrics_thread = {
        let exit = exit.clone();
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1));
                forward_stats.report();
            }
        })
    };
    thread_handles.push(report_metrics_thread);

    // let metrics_hdl = forwarder::start_forwarder_accessory_thread(
    //     deduper,
    //     metrics.clone(),
    //     args.metrics_report_interval_ms,
    //     shutdown_receiver.clone(),
    //     exit.clone(),
    // );
    // thread_handles.push(metrics_hdl);
    if use_discovery_service {
        let refresh_handle = forwarder::start_destination_refresh_thread(
            args.endpoint_discovery_url.unwrap(),
            args.discovered_endpoints_port.unwrap(),
            args.dest_ip_ports,
            unioned_dest_sockets,
            shutdown_receiver.clone(),
            exit.clone(),
        );
        thread_handles.push(refresh_handle);
    }

    if let Some(prom_bind_addr) = args.prometheus_bind_addr {
        let prom_hdl = prom::spawn_prometheus_server(
            prom_bind_addr, 
            prom_registry, 
            shutdown_receiver.clone()
        );
        thread_handles.push(prom_hdl);
    }

    info!(
        "Shredstream started, listening on {}:{}/udp.",
        args.src_bind_addr, args.src_bind_port
    );

    

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    info!(
        "Exiting Shredstream, {} received , {} sent successfully, {} failed, {} duplicate shreds.",
        metrics.agg_received_cumulative.load(Ordering::Relaxed),
        metrics
            .agg_success_forward_cumulative
            .load(Ordering::Relaxed),
        metrics.agg_fail_forward_cumulative.load(Ordering::Relaxed),
        metrics.duplicate_cumulative.load(Ordering::Relaxed),
    );
    Ok(())
}

fn start_heartbeat(
    args: ShredstreamArgs,
    exit: &Arc<AtomicBool>,
    shutdown_receiver: &Receiver<()>,
    runtime: Runtime,
    metrics: Arc<ShredMetrics>,
) -> JoinHandle<()> {
    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).unwrap_or_else(|e| {
            panic!(
                "Unable to parse keypair file. Ensure that file {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );

    heartbeat::heartbeat_loop_thread(
        args.block_engine_url.clone(),
        args.auth_url.unwrap_or(args.block_engine_url),
        auth_keypair,
        args.desired_regions,
        SocketAddr::new(
            args.common_args
                .public_ip
                .unwrap_or_else(|| get_public_ip().unwrap()),
            args.common_args.src_bind_port,
        ),
        runtime,
        "shredstream_proxy".to_string(),
        metrics,
        shutdown_receiver.clone(),
        exit.clone(),
    )
}

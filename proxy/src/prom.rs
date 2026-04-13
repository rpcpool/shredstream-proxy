use std::{net::SocketAddr, thread::JoinHandle, time::Duration};

use prometheus::{Counter, Histogram, HistogramOpts, IntCounterVec, Opts, TextEncoder};
use tiny_http::{Response, StatusCode};

lazy_static::lazy_static! {
    static ref DEDUP_DURATION_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("shredstream_dedup_duration_usec", "Time to deduplicate packets")
            .buckets(vec![0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0])
    ).unwrap();

    static ref SEND_DURATION_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("shredstream_send_duration_usec", "Time to send a batch of packets")
            .buckets(vec![0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0])
    ).unwrap();

    static ref SEND_PACKET_COUNT_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("shredstream_send_packet_count", "Number of packets per batch send (after dedup)")
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0])
    ).unwrap();

    static ref RECV_INTERVAL_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("shredstream_recv_interval_usec", "Time between receiving packet batches")
            .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0])
    ).unwrap();

    static ref RECV_PACKET_COUNT_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("shredstream_recv_packet_count", "Number of packets in incoming batch (before dedup)")
            .buckets(vec![1.0, 5.0, 10.0, 20.0, 50.0, 64.0])
    ).unwrap();

    static ref PACKETS_RECEIVED_TOTAL: Counter = Counter::new(
        "shredstream_packets_received_total", "Total packets received before dedup"
    ).unwrap();

    static ref PACKETS_DEDUPED_TOTAL: Counter = Counter::new(
        "shredstream_packets_deduped_total", "Packets filtered by dedup"
    ).unwrap();

    static ref PACKETS_FORWARDED_TOTAL: Counter = Counter::new(
        "shredstream_packets_forwarded_total", "Packets successfully forwarded"
    ).unwrap();

    static ref PACKETS_FORWARD_FAILED_TOTAL: Counter = Counter::new(
        "shredstream_packets_forward_failed_total", "Packets that failed to forward"
    ).unwrap();

    static ref PACKETS_BY_SOURCE: IntCounterVec = IntCounterVec::new(
        Opts::new("shredstream_packets_by_source", "Packets per source IP"),
        &["addr", "status"]
    ).unwrap();

    static ref ROUTING_DROP: Counter = Counter::new(
        "shredstream_routing_drop_total", "Packets dropped due to routing issues"
    ).unwrap();

    static ref ROUTING_SEND: IntCounterVec = IntCounterVec::new(
        Opts::new(

            "shredstream_routing_send_total", "Packets successfully routed to send queue"
        ),
        &["queue"]
    ).unwrap();
}

pub fn inc_routing_drop() {
    ROUTING_DROP.inc();
}

pub fn inc_routing_send<S: AsRef<str>>(queue_label: S) {
    ROUTING_SEND.with_label_values(&[queue_label.as_ref()]).inc();
}

pub fn observe_dedup_time(microseconds: f64) {
    DEDUP_DURATION_HIST.observe(microseconds);
}

pub fn observe_send_packet_count(count: f64) {
    SEND_PACKET_COUNT_HIST.observe(count);
}

pub fn observe_send_duration(microseconds: f64) {
    SEND_DURATION_HIST.observe(microseconds);
}

pub fn observe_recv_interval(microseconds: f64) {
    RECV_INTERVAL_HIST.observe(microseconds);
}

pub fn observe_recv_packet_count(count: f64) {
    RECV_PACKET_COUNT_HIST.observe(count);
}

pub fn inc_packets_received(count: u64) {
    PACKETS_RECEIVED_TOTAL.inc_by(count as f64);
}

pub fn inc_packets_deduped(count: u64) {
    PACKETS_DEDUPED_TOTAL.inc_by(count as f64);
}

pub fn inc_packets_forwarded(count: u64) {
    PACKETS_FORWARDED_TOTAL.inc_by(count as f64);
}

pub fn inc_packets_forward_failed(count: u64) {
    PACKETS_FORWARD_FAILED_TOTAL.inc_by(count as f64);
}

pub fn inc_packets_by_source(addr: &str, status: &str, count: u64) {
    PACKETS_BY_SOURCE.with_label_values(&[addr, status]).inc_by(count);
}

pub fn register_metrics(registry: &prometheus::Registry) {
    registry.register(Box::new(DEDUP_DURATION_HIST.clone())).unwrap();
    registry.register(Box::new(SEND_DURATION_HIST.clone())).unwrap();
    registry.register(Box::new(SEND_PACKET_COUNT_HIST.clone())).unwrap();
    registry.register(Box::new(RECV_INTERVAL_HIST.clone())).unwrap();
    registry.register(Box::new(RECV_PACKET_COUNT_HIST.clone())).unwrap();
    registry.register(Box::new(PACKETS_RECEIVED_TOTAL.clone())).unwrap();
    registry.register(Box::new(PACKETS_DEDUPED_TOTAL.clone())).unwrap();
    registry.register(Box::new(PACKETS_FORWARDED_TOTAL.clone())).unwrap();
    registry.register(Box::new(PACKETS_FORWARD_FAILED_TOTAL.clone())).unwrap();
    registry.register(Box::new(PACKETS_BY_SOURCE.clone())).unwrap();
    registry.register(Box::new(ROUTING_DROP.clone())).unwrap();
    registry.register(Box::new(ROUTING_SEND.clone())).unwrap();
}


pub fn spawn_prometheus_server(
    bind_addr: SocketAddr,
    registry: prometheus::Registry,
    shutdown_signal: crossbeam_channel::Receiver<()>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("ssPxyPrometheusServer".to_string())
        .spawn(move || {
            let server = tiny_http::Server::http(bind_addr).unwrap();
            log::info!("Prometheus metrics server running on {}", bind_addr);
            loop {
                if shutdown_signal.try_recv().is_ok() {
                    log::info!("Shutting down Prometheus metrics server");
                    break;
                }
                // handle each request in a separate thread to avoid blocking
                let result = server
                    .recv_timeout(Duration::from_secs(1));
                let maybe = match result {
                    Ok(r) => r,
                    Err(e) => {
                        panic!("Error receiving request: {e}");
                    }
                };
                if let Some(request) = maybe {
                    let gather = registry.gather();
                    let encoding_result = TextEncoder::new().encode_to_string(&gather);
                    match encoding_result {
                        Ok(encoded) => {
                            let response = Response::from_string(encoded)
                                .with_status_code(StatusCode::from(200));

                            if let Err(e) = request.respond(response) {
                                log::error!("Failed to respond to prometheus scrape request: {e}");
                            }
                        }
                        Err(e) => {
                            panic!("Failed to encode prometheus metrics: {e}");
                        }
                    }
                }
            }
        })
        .unwrap()
}

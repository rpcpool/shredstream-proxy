use std::{net::SocketAddr, thread::JoinHandle};

use prometheus::{Histogram, HistogramOpts, TextEncoder};
use tiny_http::{Response, StatusCode};




lazy_static::lazy_static! {
    // pub static ref REGISTRY: prometheus::Registry = prometheus::Registry::new();


    static ref DEDUP_TIME_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("dedup_time_usec", "Histogram of time taken to deduplicate entries")
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0, 233.0, 377.0, 610.0])
        
    ).unwrap();

    static ref BATCH_SEND_TIME_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("batch_send_time_usec", "Histogram of time taken to send a batch")
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0, 233.0, 377.0, 610.0])
    ).unwrap();
    
    static ref BATCH_SEND_SIZE_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("batch_send_size", "Histogram of batch_send input size")
            .buckets(vec![1.0, 5.0, 10.0, 20.0, 50.0, 70.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0])
    ).unwrap();
    
}

pub fn observe_dedup_time(microseconds: f64) {
    DEDUP_TIME_HIST.observe(microseconds);
}
pub fn observe_batch_send_size(size: f64) {
    BATCH_SEND_SIZE_HIST.observe(size);
}

pub fn observe_batch_send_time(microseconds: f64) {
    BATCH_SEND_TIME_HIST.observe(microseconds);
}

pub fn register_metrics(registry: &prometheus::Registry) {
    registry.register(Box::new(DEDUP_TIME_HIST.clone())).unwrap();
    registry.register(Box::new(BATCH_SEND_SIZE_HIST.clone())).unwrap();
    registry.register(Box::new(BATCH_SEND_TIME_HIST.clone())).unwrap();
}


pub fn spawn_prometheus_server(
    bind_addr: SocketAddr,
    registry: prometheus::Registry,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("ssPxyPrometheusServer".to_string())
        .spawn(move || {
            let server = tiny_http::Server::http(bind_addr).unwrap();
            log::info!("Prometheus metrics server running on {}", bind_addr);
            for request in server.incoming_requests() {
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
        })
        .unwrap()
}

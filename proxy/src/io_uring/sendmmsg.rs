use std::{
    collections::VecDeque,
    io, mem,
    net::{SocketAddr, UdpSocket},
    os::fd::{AsFd, AsRawFd},
    sync::{atomic::AtomicU64, Arc},
};

use core_affinity::CoreId;
use crossbeam_channel::{Sender, TrySendError};
use io_uring::{
    cqueue::{self, more, notif},
    opcode::SendZc,
    squeue::Entry,
    types, IoUring,
};
use socket2::SockAddr;

struct RegisteredBuffer {
    index: u16,
    capacity: usize,
    buf: libc::iovec,
}

impl RegisteredBuffer {
    unsafe fn unsafe_clone(&self) -> RegisteredBuffer {
        RegisteredBuffer {
            index: self.index,
            capacity: self.capacity,
            buf: self.buf.clone(),
        }
    }
}

unsafe impl Send for RegisteredBuffer {}

pub struct FreshSendPermit {
    rbuf: Option<RegisteredBuffer>,
    free_rbuf_tx: crossbeam_channel::Sender<FreshSendPermit>,
}

impl Drop for FreshSendPermit {
    fn drop(&mut self) {
        if let Some(rbuf) = self.rbuf.take() {
            // Return the buffer to the free buffer channel
            let new_permit = FreshSendPermit {
                rbuf: Some(rbuf),
                free_rbuf_tx: self.free_rbuf_tx.clone(),
            };
            match self.free_rbuf_tx.try_send(new_permit) {
                Err(TrySendError::Full(_)) => {
                    unreachable!("Free buffer channel should not be full");
                }
                _ => {}
            }
        }
    }
}

struct FilledSendPermit {
    rbuf: Option<RegisteredBuffer>,
    free_rbuf_tx: crossbeam_channel::Sender<FreshSendPermit>,
}

impl Drop for FilledSendPermit {
    fn drop(&mut self) {
        if let Some(rbuf) = self.rbuf.take() {
            // Return the buffer to the free buffer channel
            let new_permit = FreshSendPermit {
                rbuf: Some(rbuf),
                free_rbuf_tx: self.free_rbuf_tx.clone(),
            };
            match self.free_rbuf_tx.try_send(new_permit) {
                Err(TrySendError::Full(_)) => {
                    unreachable!("Free buffer channel should not be full");
                }
                _ => {}
            }
        }
    }
}

impl FilledSendPermit {
    fn send(
        mut self,
        dests: &[SocketAddr],
        send_req_tx: &Sender<SendReqBatch>,
    ) -> Result<(), SendError> {
        let rbuf = self.rbuf.take().expect("send");
        let send_req = SendReq {
            dests: Vec::from_iter(dests.iter().cloned()),
            rbuf,
        };
        let send_req_batch = SendReqBatch {
            batch: vec![send_req],
        };
        send_req_tx
            .send(send_req_batch)
            .map_err(|_| SendError::Disconnected)?;
        Ok(())
    }
}

impl FreshSendPermit {
    fn store<Src>(mut self, src: Src) -> Result<FilledSendPermit, WriteError>
    where
        Src: AsRef<[u8]>,
    {
        if let Some(rbuf) = &mut self.rbuf {
            rbuf.store(src)?;
        } else {
            unreachable!("MulticastSendPermit should always have a buffer");
        }

        let rbuf = self.rbuf.take().expect("store");
        let stored_permit = FilledSendPermit {
            rbuf: Some(rbuf),
            free_rbuf_tx: self.free_rbuf_tx.clone(),
        };
        Ok(stored_permit)
    }

    fn send<Src>(
        self,
        data: Src,
        dests: &[SocketAddr],
        send_req_tx: &Sender<SendReqBatch>,
    ) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let stored_permit = self.store(data)?;
        stored_permit.send(dests, send_req_tx)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteError {
    #[error("attempt to write more than the buffer capacity")]
    Overflow,
}

impl RegisteredBuffer {
    pub fn store<Src>(&mut self, src: Src) -> Result<(), WriteError>
    where
        Src: AsRef<[u8]>,
    {
        let src_len = src.as_ref().len();
        if src_len > self.capacity {
            return Err(WriteError::Overflow);
        }
        unsafe {
            let slice = std::slice::from_raw_parts_mut(self.buf.iov_base as *mut u8, self.capacity);
            slice[..src_len].copy_from_slice(src.as_ref());
            self.buf.iov_len = src_len as _;
        }
        Ok(())
    }
}

pub struct SendReqBatch {
    batch: Vec<SendReq>,
}

pub struct SendReq {
    pub dests: Vec<SocketAddr>,
    rbuf: RegisteredBuffer,
}


#[derive(Debug)]
struct SendReqContext {
    dest: SockAddr,
    rbuf_index: u16,
}

struct RegisteredBufferRc {
    rbuf: RegisteredBuffer,
    rc: u16,
}

struct IoUringEvLoop {
    ring: IoUring,
    #[allow(dead_code)]
    send_socket: UdpSocket,
    send_socket_fd: types::Fd,
    backpressured_submissions: VecDeque<Entry>,
    free_buffer_tx: crossbeam_channel::Sender<FreshSendPermit>,
    buffer_capacity: usize,
    send_req_rx: crossbeam_channel::Receiver<SendReqBatch>,
    registered_buffers_refcount_vec: Vec<RegisteredBufferRc>,
    linger: usize,
    stats: Arc<SendStats>,
    disconnected: bool,
}

#[derive(Debug, Default)]
struct SendStats {
    bytes_sent: AtomicU64,
    errono_count: AtomicU64,
    zc_send_count: AtomicU64,
    copied_send_count: AtomicU64,
    submit_count: AtomicU64,
    submit_wait_cumu_time_ms: AtomicU64,
    inflight_send: AtomicU64,
}

impl IoUringEvLoop {
    fn push_send_req_batch(&mut self, send_req: SendReqBatch) -> usize {
        send_req
            .batch
            .into_iter()
            .fold(0, |acc, req| acc + self.push_send_req(req))
    }

    fn push_send_req(&mut self, send_req: SendReq) -> usize {
        let mut total_submissions = 0;
        let SendReq { dests, rbuf } = send_req;
        let buf_idx = rbuf.index;
        let rbuf_ref_counter = dests.len();
        // Copied from io-uring crate private source code.
        const IORING_SEND_ZC_REPORT_USAGE: u16 = 8;

        for dest in dests {
            log::trace!("Building send operation to dest: {:?}", dest);
            // let libc_dest_socket = into_libc_socketaddr(dest);
            let sockaddr = SockAddr::from(dest);
            println!("sockaddr: {:?}, {}", sockaddr, sockaddr.len());

            let ctx = SendReqContext {
                dest: sockaddr,
                rbuf_index: buf_idx,
            };

            let ctx = Box::new(ctx);

            let op = SendZc::new(
                self.send_socket_fd,
                rbuf.buf.iov_base as *const _,
                rbuf.buf.iov_len as _,
            )
            .buf_index(Some(buf_idx))
            .dest_addr(ctx.dest.as_ptr() as *const _)
            .dest_addr_len(ctx.dest.len())
            .zc_flags(IORING_SEND_ZC_REPORT_USAGE)
            .build();

            // THIS IS A HUGE HACK, since `dest_addr` must be a pointer,
            // I need to point to a memory location that is valid for the lifetime of the operation.
            // So I use the `user_data` field to store a pointer to the context.
            let ctx_ptr = Box::into_raw(ctx) as *const libc::c_void;
            let entry = op.user_data(ctx_ptr as _);

            let submission_result = unsafe { self.ring.submission().push(&entry) };

            match submission_result {
                Ok(_) => {
                    // Successfully pushed the operation
                    self.stats.inflight_send.fetch_add(
                        1,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    total_submissions += 1;
                }
                Err(_e) => {
                    // If we can't push, we need to handle backpressure
                    self.backpressured_submissions.push_back(entry);
                }
            }
        }
        let rbuf_idx = rbuf.index as usize;
        self.registered_buffers_refcount_vec[rbuf_idx].rc += rbuf_ref_counter as u16;
        total_submissions
    }

    fn try_drain_backpressured_submissions(&mut self) -> usize {
        let mut total_submissions = 0;
        loop {
            if self.ring.completion().is_full() {
                // If the completion queue is full, we can't push more operations
                break;
            }
            let Some(op) = self.backpressured_submissions.pop_front() else {
                // No more backpressured submissions to process
                break;
            };
            match unsafe { self.ring.submission().push(&op) } {
                Ok(_) => {
                    // Successfully pushed the operation
                    total_submissions += 1;
                }
                Err(_e) => {
                    // If we can't push, we need to stop trying
                    self.backpressured_submissions.push_front(op);
                    break;
                }
            }
        }
        total_submissions
    }

    fn dec_used_buffer_ref_cnt(&mut self, buf_index: u16) {
        let rbuf = &mut self.registered_buffers_refcount_vec[buf_index as usize];
        // Decrease the reference counter
        rbuf.rc = rbuf
            .rc
            .checked_sub(1)
            .expect("release_used_buffer");
        log::trace!("registered buf {} - {} refs", buf_index, rbuf.rc);
        if rbuf.rc == 0 {
            log::trace!("Releasing used buffer: {}", rbuf.rbuf.index);
            // The channel capacity should be enough to hold all buffers
            // so we can safely send the buffer back to the free buffer channel
            let rbuf2 = unsafe { rbuf.rbuf.unsafe_clone() };
            let new_permit = FreshSendPermit {
                rbuf: Some(rbuf2),
                free_rbuf_tx: self.free_buffer_tx.clone(),
            };
            self.free_buffer_tx
                .try_send(new_permit)
                .expect("release_used_buffer");
        }
    }

    fn process_completion_queue(&mut self) {
        enum CqeResult {
            More {
                byte_sent: u32,
                ctx: *const SendReqContext,
                err: Option<std::io::Error>,
            },
            Completed {
                ctx: Box<SendReqContext>,
                zero_copy: bool,
                err: Option<std::io::Error>,
            },
        }

        impl From<cqueue::Entry> for CqeResult {
            fn from(cqe: cqueue::Entry) -> Self {
                let res = cqe.result();
                let user_data = cqe.user_data();
                let ctx_ptr = user_data as *const SendReqContext;
                let err = if res < 0 {
                    Some(std::io::Error::from_raw_os_error(-res))
                } else {
                    None
                };
                if more(cqe.flags()) {
                    CqeResult::More {
                        byte_sent: res as _,
                        ctx: ctx_ptr,
                        err,
                    }
                } else if notif(cqe.flags()) {
                    assert!(notif(cqe.flags()));
                    let boxed_ctx = unsafe { Box::from_raw(ctx_ptr as *mut SendReqContext) };
                    log::trace!("SendZc completed with flags: {}, user_data: {:?}", cqe.flags(), boxed_ctx);
                    CqeResult::Completed {
                        ctx: boxed_ctx,
                        zero_copy: true,
                        err,
                    }
                } else {
                    panic!("cqe.flags()")
                }
            }
        }

        let cqe_vec = {
            let mut cq= self.ring.completion();
            cq.sync();
            // TODO: create a slab for this
            let mut cqe_vec = Vec::with_capacity(cq.len());
            for cqe in cq {
                let cqe = cqe.into();
                cqe_vec.push(cqe);
            }
            cqe_vec
        };
        
        for cqe in cqe_vec {
            match cqe {
                CqeResult::More { byte_sent, ctx: _, err } => {
                    log::trace!("SendZc more: {} bytes sent", byte_sent);
                    self.stats
                        .bytes_sent
                        .fetch_add(byte_sent as _, std::sync::atomic::Ordering::Relaxed);
                    self.stats
                        .zc_send_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if let Some(e) = err {
                        self.stats.errono_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        log::error!("SendZc failed with -errno: {e:?}");
                    }
                }
                CqeResult::Completed { ctx, zero_copy , err } => {
                    self.dec_used_buffer_ref_cnt(ctx.rbuf_index);
                    if zero_copy {
                        log::trace!("SendZc completed with zero-copy");
                        self.stats
                            .zc_send_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        log::debug!("sendzc failed to used zero-copy");
                        self.stats
                            .copied_send_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    if let Some(e) = err {
                        log::error!("SendZc failed with -errno: {e:?}");
                        self.stats.errono_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn fill_submissions(&mut self) -> usize {
        if self.ring.completion().is_full() {
            return 0;
        }
        let mut total_submissions = 0;
        self.try_drain_backpressured_submissions();

        while !self.ring.submission().is_full() {
            let send_req = match self.send_req_rx.try_recv() {
                Ok(req) => req,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    return total_submissions;
                },
            };
            total_submissions += self.push_send_req_batch(send_req);
        }
        total_submissions
    }

    fn drain_process_cqe(&mut self) {
        log::debug!("Draining completion queue");
        while self.stats.inflight_send.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            self.process_completion_queue();
        }
    }

    pub fn ev_loop(mut self) {
        while !self.disconnected {
            self.process_completion_queue();
            let total_submission = self.fill_submissions();
            if total_submission > 0 {
                log::trace!("Filled {total_submission} submissions");
            }
            if self.ring.submission().len() >= self.linger {
                let t = std::time::Instant::now();
                let submitted = self.ring.submit().expect("submit failed");
                let elapsed = t.elapsed();
                self.stats
                    .submit_count
                    .fetch_add(submitted as u64, std::sync::atomic::Ordering::Relaxed);
                self.stats.submit_wait_cumu_time_ms.fetch_add(
                    elapsed.as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                log::trace!("Submitted {submitted} requests in {elapsed:?}");
            }
        }

        self.drain_process_cqe();
    }
}

///
/// Zero-Copy Multicast Sender
#[derive(Debug, Clone)]
pub struct ZcMulticastSender {
    free_buffer_rx: crossbeam_channel::Receiver<FreshSendPermit>,
    send_req_tx: crossbeam_channel::Sender<SendReqBatch>,
}

pub const DEFAULT_NUM_REGISTERED_BUFFERS: u16 = 10000;
pub const DEFAULT_MTU_SIZE: usize = 1500;
pub const DEFAULT_IORING_QUEUE_SIZE: u32 = 1024;
pub const DEFAULT_LINGER: usize = 1;

#[derive(Debug, Clone)]
pub struct ZcMulticastSenderConfig {
    num_registered_buffers: u16,
    registered_buffer_size: usize,
    ///
    /// Queue depth of the submission queue.
    /// Note: must be a power of two.
    ioring_queue_size: u32,
    linger: usize,
    core_id: CoreId,
}

impl Default for ZcMulticastSenderConfig {
    fn default() -> Self {
        ZcMulticastSenderConfig {
            num_registered_buffers: DEFAULT_NUM_REGISTERED_BUFFERS,
            registered_buffer_size: DEFAULT_MTU_SIZE,
            ioring_queue_size: DEFAULT_IORING_QUEUE_SIZE,
            linger: DEFAULT_LINGER,
            core_id: core_affinity::get_core_ids().unwrap()[0],
        }
    }
}

impl ZcMulticastSenderConfig {
    pub fn num_registered_buffers(mut self, num: u16) -> Self {
        self.num_registered_buffers = num;
        self
    }

    pub fn registered_buffer_size(mut self, size: usize) -> Self {
        self.registered_buffer_size = size;
        self
    }

    pub fn linger(mut self, linger: usize) -> Self {
        self.linger = linger;
        self
    }

    pub fn core_id(mut self, core_id: CoreId) -> Self {
        self.core_id = core_id;
        self
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    #[error("disconnected from io-uring event loop")]
    Disconnected,
    #[error(transparent)]
    WriteError(#[from] WriteError),
}

struct SendReqBatcher {
    batch: Vec<FilledSendPermit>,
    dests: Vec<SocketAddr>,
}

impl SendReqBatcher {
    fn send(self, send_req_tx: &Sender<SendReqBatch>) -> Result<(), SendError> {
        let batch = self
            .batch
            .into_iter()
            .map(|mut stored_permit| {
                let rbuf = stored_permit.rbuf.take().expect("into_send_req_batch");
                SendReq {
                    dests: self.dests.clone(),
                    rbuf,
                }
            })
            .collect();
        send_req_tx
            .send(SendReqBatch { batch })
            .map_err(|_| SendError::Disconnected)
    }
}

impl ZcMulticastSender {
    pub fn send<Src>(&self, src: Src, dests: &[SocketAddr]) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let data = src.as_ref();
        let permit = self
            .free_buffer_rx
            .recv()
            .map_err(|_| SendError::Disconnected)?;
        permit.send(data, dests, &self.send_req_tx)
    }

    ///
    /// Cross-product between the src_vec and dests.
    /// Each src in src_vec is sent to each dest in dests.
    /// This function will block until all messages are sent.
    ///
    /// Try to build the biggest batch possible if enough permits are available to do so.
    pub fn cross_batch_send<Src>(
        &self,
        src_vec: &[Src],
        dests: &[SocketAddr],
    ) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let mut num_msg = src_vec.len();

        let mut permits = Vec::with_capacity(num_msg);
        let mut src_i: usize = 0;
        while num_msg > 0 {
            'inner: while num_msg > 0 {
                match self.free_buffer_rx.try_recv() {
                    Ok(permit) => {
                        permits.push(permit);
                        num_msg -= 1;
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        break 'inner;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        return Err(SendError::Disconnected);
                    }
                }
            }

            if permits.is_empty() {
                // prepare to block until at least one permit is available
                match self.free_buffer_rx.recv() {
                    Ok(permit) => {
                        permits.push(permit);
                        num_msg -= 1;
                    }
                    Err(_) => {
                        return Err(SendError::Disconnected);
                    }
                }
            }
            let mut batcher = SendReqBatcher {
                batch: Vec::with_capacity(permits.len()),
                dests: Vec::from_iter(dests.iter().cloned()),
            };
            for permit in permits.drain(..) {
                let data = src_vec.get(src_i).expect("batch_send");
                src_i += 1;
                let stored = permit.store(data)?;
                batcher.batch.push(stored);
            }
            batcher.send(&self.send_req_tx)?;
        }
        Ok(())
    }
}

pub fn zc_multicast_sender(send_socket: UdpSocket) -> io::Result<ZcMulticastSender> {
    zc_multicast_sender_with_config(send_socket, ZcMulticastSenderConfig::default())
}

pub fn zc_multicast_sender_with_config(
    send_socket: UdpSocket,
    config: ZcMulticastSenderConfig,
) -> io::Result<ZcMulticastSender> {
    let ZcMulticastSenderConfig {
        num_registered_buffers,
        registered_buffer_size,
        ioring_queue_size,
        linger,
        core_id,
    } = config;

    let ring = IoUring::builder()
        .setup_cqsize(ioring_queue_size * 2)
        .build(ioring_queue_size)?;
    log::trace!(
        "zc_multicast_sender_with_config: io_uring queue size: {}",
        ioring_queue_size
    );
    let (send_req_tx, send_req_rx) = crossbeam_channel::bounded(1000);
    let (free_buffer_tx, free_buffer_rx) = crossbeam_channel::bounded(num_registered_buffers as _);

    let mut rbuf_vec = Vec::with_capacity(num_registered_buffers as _);
    for i in 0..num_registered_buffers {
        // intermediate registered buffer
        let mut irbuf = vec![0u8; registered_buffer_size];
        let rbuf = RegisteredBuffer {
            index: i as u16,
            capacity: registered_buffer_size,
            buf: libc::iovec {
                iov_base: irbuf.as_mut_ptr() as *mut _,
                iov_len: registered_buffer_size,
            },
        };
        mem::forget(irbuf); // Prevent the buffer from being dropped
        rbuf_vec.push(rbuf);
    }

    let mut iovecs = Vec::with_capacity(num_registered_buffers as _);
    for rbuf in rbuf_vec.iter() {
        let iovec = rbuf.buf.clone();
        iovecs.push(iovec);
    }

    unsafe { ring.submitter().register_buffers(&iovecs)? };
    log::trace!(
        "zc_multicast_sender_with_config: registered {} buffers of size {}",
        num_registered_buffers,
        registered_buffer_size
    );  

    let mut rbuf_rc_vec = vec![];
    for rbuf in rbuf_vec {
        rbuf_rc_vec.push(RegisteredBufferRc {
            rbuf: unsafe { rbuf.unsafe_clone() },
            rc: 0,
        });
        let permit = FreshSendPermit {
            rbuf: Some(rbuf),
            free_rbuf_tx: free_buffer_tx.clone(),
        };
        free_buffer_tx
            .try_send(permit)
            .expect("Failed to send registered buffer");
    }

    let send_socket_fd = types::Fd(send_socket.as_fd().as_raw_fd());
    let stats = Arc::new(SendStats::default());
    let ev_loop = IoUringEvLoop {
        ring,
        send_socket,
        send_socket_fd,
        backpressured_submissions: VecDeque::with_capacity(num_registered_buffers as _),
        free_buffer_tx,
        buffer_capacity: registered_buffer_size,
        send_req_rx,
        registered_buffers_refcount_vec: rbuf_rc_vec,
        linger,
        stats: Arc::clone(&stats),
        disconnected: false,
    };

    log::trace!(
        "zc_multicast_sender_with_config: starting event loop on core {:?}",
        core_id
    );
    let _jh = std::thread::spawn(move || {
        core_affinity::set_for_current(core_id);
        ev_loop.ev_loop();
    });

    Ok(ZcMulticastSender {
        free_buffer_rx,
        send_req_tx,
    })
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use log::{LevelFilter, Metadata, Record, SetLoggerError};
    use rand::{thread_rng, RngCore};
    use solana_net_utils::bind_to_localhost;
    use solana_streamer::{packet::Packet, recvmmsg::recv_mmsg};

    use crate::io_uring::sendmmsg::zc_multicast_sender;

    static LOGGER: SimpleLogger = SimpleLogger;

    pub fn init() -> Result<(), SetLoggerError> {
        log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Trace))
    }

    struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= log::max_level()
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!("{} - line_no: {} - {}", record.level(), record.line().unwrap_or(0), record.args());
            }
        }

        fn flush(&self) {}
    }

    #[test]
    pub fn test_send_one_msg() {
        init().expect("Failed to initialize logger");
        log::error!("Starting multicast message test");
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();

        // let reader2 = bind_to_localhost().expect("bind");
        // let addr2 = reader2.local_addr().unwrap();

        // let reader3 = bind_to_localhost().expect("bind");
        // let addr3 = reader3.local_addr().unwrap();

        // let reader4 = bind_to_localhost().expect("bind");
        // let addr4 = reader4.local_addr().unwrap();
        // let sender_socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        // let socket_type = libc::SOCK_DGRAM | libc::SOCK_CLOEXEC;
        // let sender_socket = socket2::Socket::new(libc::AF_INET.into(), socket_type.into(), None)
        //     .expect("create sender socket");
        let sender_socket = bind_to_localhost().expect("bind");
        let mc_sender = zc_multicast_sender(sender_socket).expect("zc_multicast_sender");
        let packet = Packet::default();
        log::trace!("dest1: {:?}", addr);
        mc_sender
            .send(packet.data(..).unwrap(), &[addr])
            .expect("send multicast");

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap();
        assert_eq!(1, recv);

        log::trace!("Received {} packets", recv);
    }

    #[test]
    pub fn test_multicast() {
        init().expect("Failed to initialize logger");
        log::error!("Starting multicast message test");
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = bind_to_localhost().expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let reader3 = bind_to_localhost().expect("bind");
        let addr3 = reader3.local_addr().unwrap();

        let reader4 = bind_to_localhost().expect("bind");
        let addr4 = reader4.local_addr().unwrap();

        let sender_socket = bind_to_localhost().expect("bind");
        let mc_sender = zc_multicast_sender(sender_socket).expect("zc_multicast_sender");
        let mut expected_packet = vec![0u8; 1232];

        let mut rng = thread_rng();
        rng.fill_bytes(expected_packet.as_mut_slice());

        log::trace!("dest1: {:?}", addr);
        mc_sender
            .send(&expected_packet, &[addr, addr2, addr3, addr4])
            .expect("send multicast");


        for rdr  in [reader, reader2, reader3, reader4].iter() {
            let mut packets = vec![Packet::default(); 32];
            let recv = recv_mmsg(rdr, &mut packets[..]).unwrap();
            assert_eq!(1, recv);
            let actual = packets[0].data(..).unwrap();
            assert_eq!(actual.len(), expected_packet.len());
            assert_eq!(*actual, expected_packet[..]);
        }

        thread::sleep(Duration::from_secs(1));
    }
}

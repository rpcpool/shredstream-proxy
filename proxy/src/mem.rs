use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    ops::{Index, IndexMut},
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, Thread},
};

use bytes::{Buf, BufMut, buf::UninitSlice};

#[derive(Debug, thiserror::Error)]
#[error("allocation error")]
pub struct AllocError;

#[repr(C)]
pub struct SharedMem {
    ptr: *mut u8,
    aligned_size: usize,
    capacity: usize,
}

pub fn try_alloc_shared_mem(
    align: usize,
    capacity: usize,
    huge: bool,
) -> Result<*mut u8, AllocError> {
    // assert!(align.is_power_of_two(), "alignment must be a power of two");
    assert!(
        capacity.is_power_of_two(),
        "capacity must be a power of two"
    );
    let aligned_size = capacity * align;
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            aligned_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS | if huge { libc::MAP_HUGETLB } else { 0 },
            -1,
            0,
        )
    };

    if std::ptr::eq(ptr, libc::MAP_FAILED) {
        return Err(AllocError);
    }

    // zero initialize the memory
    unsafe {
        std::ptr::write_bytes(ptr as *mut u8, 0, aligned_size);
    }

    Ok(ptr as *mut u8)
}



impl SharedMem {
    fn new(element_size: usize, capacity: usize, huge: bool) -> Result<Self, AllocError> {
        let ptr = try_alloc_shared_mem(element_size, capacity, huge)?;
        let aligned_size = capacity * element_size;

        Ok(Self {
            ptr,
            aligned_size,
            capacity,
        })
    }

    fn dealloc(&self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.aligned_size);
        }
    }
}

impl Drop for SharedMem {
    fn drop(&mut self) {
        self.dealloc();
    }
}

#[derive(Debug)]
#[repr(C, align(16))]
pub struct FrameDesc {
    pub ptr: *mut u8,
    pub frame_size: usize,
}

#[derive(Debug)]
#[repr(C, align(32))]
pub struct FrameBufMut {
    ptr: *mut u8,
    desc: FrameDesc,
}

unsafe impl Send for FrameBufMut {}

#[derive(Debug)]
#[repr(C, align(32))]
pub struct FrameBuf {
    curr_ptr: *mut u8,
    len: usize,
    desc: FrameDesc,
}

impl FrameBuf {
    #[inline]
    pub fn len(&self) -> usize {
        let end = unsafe { self.desc.ptr.add(self.len) };
        (end as usize) - (self.curr_ptr as usize)
    }
}

unsafe impl Send for FrameBuf {}

impl From<FrameBufMut> for FrameBuf {
    fn from(buf_mut: FrameBufMut) -> Self {
        let len = (buf_mut.ptr as usize) - (buf_mut.desc.ptr as usize);
        Self {
            curr_ptr: buf_mut.desc.ptr,
            len,
            desc: buf_mut.desc,
        }
    }
}

impl FrameDesc {
    pub fn as_mut_buf(&self) -> FrameBufMut {
        FrameBufMut {
            ptr: self.ptr,
            desc: FrameDesc {
                ptr: self.ptr,
                frame_size: self.frame_size,
            },
        }
    }
}

impl From<FrameDesc> for FrameBufMut {
    fn from(desc: FrameDesc) -> Self {
        Self {
            ptr: desc.ptr,
            desc,
        }
    }
}


impl FrameBufMut {
    #[inline]
    pub fn base(&self) -> *mut u8 {
        ((self.ptr as usize) & !(self.desc.frame_size - 1)) as *mut u8
    }

    pub fn len(&self) -> usize {
        let base = self.base() as usize;
        (self.ptr as usize) - base
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.desc.frame_size
    }

    #[inline]
    pub fn cast_to<T>(&self) -> *mut T {
        self.base() as *mut T
    }

    #[inline]
    fn end_ptr(&self) -> *const u8 {
        unsafe { self.base().add(self.capacity()) }
    }
}

unsafe impl BufMut for FrameBufMut {
    fn remaining_mut(&self) -> usize {
        // given that ptr must always aligned with `frame_align`,
        // we just be able to infer the remaining mut size from frame_align
        let frame_offset = (self.ptr as usize) & (self.desc.frame_size - 1);
        self.desc.frame_size - frame_offset
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        let new_ptr = self.ptr.add(cnt);
        assert!(
            new_ptr as *const u8 <= self.end_ptr(),
            "advance_mut out of bounds"
        );
        self.ptr = new_ptr;
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        unsafe { UninitSlice::from_raw_parts_mut(self.ptr, self.remaining_mut()) }
    }
}

impl Buf for FrameBuf {
    fn remaining(&self) -> usize {
        let end = unsafe { self.desc.ptr.add(self.len) };
        (end as usize) - (self.curr_ptr as usize)
    }

    fn chunk(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.curr_ptr, self.remaining()) }
    }

    fn advance(&mut self, cnt: usize) {
        let new_ptr = unsafe { self.curr_ptr.add(cnt) };
        let end = unsafe { self.desc.ptr.add(self.len) };
        assert!(
            new_ptr as *const u8 <= end,
            "advance out of bounds"
        );
        self.curr_ptr = new_ptr;
    }
}

use std::{ptr, sync::atomic::AtomicBool};

// We wrap T to include a 'ready' flag for each slot
#[repr(C, align(64))]
struct Slot<T: Sized> {
    data: std::mem::MaybeUninit<T>,
    is_ready: AtomicBool,
}

struct RingInner<T> {
    buf: *mut Slot<T>, // Changed to Slot<T>
    capacity: usize,
    mask: usize,
    head: AtomicUsize, // Producer index (reserved)
    tail: AtomicUsize, // Consumer index
    futex_flag: AtomicI32,
    shmem: Option<SharedMem>,
}

impl<T> Drop for RingInner<T> {
    fn drop(&mut self) {
        if let Some(shmem) = self.shmem.take() {
            let mut tail = self.tail.load(Ordering::Acquire);
            let head = self.head.load(Ordering::Acquire);

            // Drop initialized slots
            while tail != head {
                unsafe {
                    let slot = &mut *self.buf.add(tail & self.mask);
                    if slot.is_ready.load(Ordering::Acquire) {
                        ptr::drop_in_place(slot.data.as_mut_ptr());
                    }
                }
                tail = tail.wrapping_add(1);
            }

            drop(shmem);
        }
    }
}

unsafe impl<T: Send> Send for RingInner<T> {}
unsafe impl<T: Send> Sync for RingInner<T> {}

pub struct Tx<T> {
    inner: Arc<RingInner<T>>,
}

impl<T> Clone for Tx<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct Rx<T> {
    inner: Arc<RingInner<T>>,
}

pub fn message_ring<T>(capacity: usize) -> Result<(Tx<T>, Rx<T>), AllocError> {
    let capacity = capacity.next_power_of_two();
    let align = std::mem::size_of::<Slot<T>>();

    // Allocate memory for Slots
    let shmem = SharedMem::new(align, capacity, false)?;
    let ptr = shmem.ptr as *mut Slot<T>;
    // Initialize the is_ready flags to false
    for i in 0..capacity {
        unsafe {
            let slot_ptr = ptr.add(i);
            ptr::write(&mut (*slot_ptr).is_ready, AtomicBool::new(false));
        }
    }

    let inner = Arc::new(RingInner {
        buf: ptr,
        capacity,
        mask: capacity - 1,
        head: AtomicUsize::new(0),
        tail: AtomicUsize::new(0),
        futex_flag: AtomicI32::new(0),
        shmem: Some(shmem),
    });

    Ok((
        Tx {
            inner: Arc::clone(&inner),
        },
        Rx { inner },
    ))
}

impl<T> Tx<T> {
    pub fn send(&self, value: T) -> Result<(), T> {
        loop {
            // 1. Load head and tail to check if the ring is full.
            // head: Relaxed is okay here because it's only a hint for the CAS.
            // tail: Acquire is REQUIRED to ensure we don't overwrite data
            //       the consumer hasn't finished reading yet.
            let head = self.inner.head.load(Ordering::Relaxed);
            let tail = self.inner.tail.load(Ordering::Acquire);

            if head.wrapping_sub(tail) >= self.inner.capacity {
                return Err(value); // Ring is full
            }

            // 2. Claim a slot using Compare-and-Swap (CAS).
            // We use SeqCst or AcqRel here to ensure that once we "win" this slot,
            // we have a synchronized view of the memory.
            if self
                .inner
                .head
                .compare_exchange_weak(head, head + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    // 3. Calculate slot location.
                    let slot = &*self.inner.buf.add(head & self.inner.mask);

                    // 4. Write the data into the MaybeUninit.
                    // We use .write() which is a wrapper for ptr::write.
                    ptr::write(slot.data.as_ptr() as *mut T, value);

                    // 5. RELEASE the data to the consumer.
                    // This store ensures the data write above is visible to
                    // any thread that performs an Acquire load on is_ready.
                    slot.is_ready.store(true, Ordering::Release);
                }

                // 6. Futex Wake Logic.
                // If the consumer is sleeping (futex_flag == 0), we wake them.
                // We use Release to ensure the flag update is visible.
                if self.inner.futex_flag.swap(1, Ordering::Release) == 0 {
                    unsafe {
                        libc::syscall(
                            libc::SYS_futex,
                            &self.inner.futex_flag as *const AtomicI32,
                            libc::FUTEX_WAKE,
                            1, // Wake 1 thread
                        );
                    }
                }
                return Ok(());
            }
            // If CAS failed, another producer grabbed 'head'.
            // The loop will retry with the new head value.
            std::hint::spin_loop();
        }
    }
}

impl<T> Rx<T> {
    pub fn recv(&mut self) -> T {
        for _ in 0..999 {
            if let Some(val) = self.try_recv() {
                return val;
            }
            spin_loop();
        }

        loop {
            if let Some(val) = self.try_recv() {
                return val;
            }

            self.inner.futex_flag.store(0, Ordering::SeqCst);

            if let Some(val) = self.try_recv() {
                return val;
            }

            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    &self.inner.futex_flag as *const AtomicI32,
                    libc::FUTEX_WAIT,
                    0,
                    std::ptr::null::<libc::timespec>(),
                );
            }
        }
    }

    pub fn try_recv(&mut self) -> Option<T> {
        let tail = self.inner.tail.load(Ordering::Relaxed);

        unsafe {
            let slot = &*self.inner.buf.add(tail & self.inner.mask);

            // IMPORTANT: In MPSC, even if head > tail, the data at tail might
            // not be written yet because the producer was interrupted.
            if !slot.is_ready.load(Ordering::Acquire) {
                return None;
            }

            let val = ptr::read(slot.data.as_ptr());

            // Reset the flag for the next time this slot is used
            slot.is_ready.store(false, Ordering::Release);

            // Increment tail to free the slot
            self.inner.tail.store(tail + 1, Ordering::Release);
            Some(val)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Barrier};

    use super::*;

    #[test]
    fn test_mpsc_contention() {
        let capacity = 1024;
        let (tx, mut rx) = message_ring::<usize>(capacity).unwrap();

        let num_producers = 4;
        let msgs_per_producer = 1000;
        let barrier = Arc::new(Barrier::new(num_producers + 1));
        let mut handles = Vec::new();

        // Start Producers
        for p in 0..num_producers {
            let tx_clone = tx.clone();
            let b_clone = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b_clone.wait(); // Synchronize start
                for i in 0..msgs_per_producer {
                    let val = p * 10000 + i;
                    while tx_clone.send(val).is_err() {
                        spin_loop(); // Wait if ring is full
                    }
                }
            }));
        }

        barrier.wait(); // Start everyone at once

        let mut received = HashSet::new();
        let total_expected = num_producers * msgs_per_producer;

        for _ in 0..total_expected {
            received.insert(rx.recv());
        }

        assert_eq!(received.len(), total_expected);
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_frame_buffer_lifecycle() {
        let align = 4096;
        let capacity = 1;
        // 1. Setup the memory pool
        let mem = SharedMem::new(align, capacity, false).unwrap();

        // At this point, the fill_ring inside PagedAlignedMem logic
        // should have been populated. Let's create our own handles for testing.
        let (tx_fill, mut rx_fill) = message_ring::<FrameDesc>(capacity).unwrap();
        let (rx_tx, mut rx_rx) = message_ring::<FrameDesc>(capacity).unwrap();
        // Manually push frames into our test fill ring
        for i in 0..capacity {
            tx_fill
                .send(FrameDesc {
                    ptr: unsafe { mem.ptr.add(i * align) },
                    frame_size: align,
                })
                .unwrap();
        }

        // 2. Simulate taking a frame from the pool
        let desc = rx_fill.recv();
        let expected_ptr = desc.ptr;
        println!("Received frame at ptr: {:p}", expected_ptr);
        let mut buf = desc.as_mut_buf();
        assert_eq!(buf.remaining_mut(), 4096);
        buf.put_u32(0xDEADBEEF);
        assert_eq!(buf.remaining_mut(), 4092);

        rx_tx.send(desc).unwrap();

        // 3. Verify the frame returned to the fill ring
        let returned_desc = rx_rx.recv();
        assert_eq!(returned_desc.ptr, expected_ptr);
        // 4. Verify the frame is zeroed out
    }

    #[test]
    fn test_blocking_recv() {
        let (tx, mut rx) = message_ring::<u64>(16).unwrap();

        let handle = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(200));
            tx.send(42).unwrap();
        });

        let start = std::time::Instant::now();
        let val = rx.recv(); // Should block for ~200ms

        assert_eq!(val, 42);
        assert!(start.elapsed().as_millis() >= 200);
        handle.join().unwrap();
    }

    #[test]
    fn test_buf_and_bufmut_impls() {
        let frame_size = 4096;
        let shmem = SharedMem::new(frame_size, 1, false).unwrap();
        let desc = FrameDesc {
            ptr: shmem.ptr,
            frame_size,
        };

        let mut buf_mut: FrameBufMut = desc.into();
        assert_eq!(buf_mut.remaining_mut(), 4096);
        buf_mut.put_slice(&[1, 2, 3, 4]);
        assert_eq!(buf_mut.remaining_mut(), 4092);
        assert_eq!(buf_mut.len(), 4);

        let mut buf: FrameBuf = buf_mut.into();
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.remaining(), 4);
        let chunk = buf.chunk();
        assert_eq!(chunk, &[1, 2, 3, 4]);
        buf.advance(4);
        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.len(), 0)
    }
}

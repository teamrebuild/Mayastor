//! The I/O Arbiter manages I/O access to a segment.
//! An I/O is held off if a segment is already in use otherwise it is permitted.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct IoArbiter {
    io_count: AtomicU64,
}

impl IoArbiter {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn register(&self) {
        self.inc_io_count();
    }

    pub fn unregister(&self) {
        self.dec_io_count();
    }

    fn inc_io_count(&self) {
        loop {
            let previous = self.io_count.load(Ordering::Relaxed);
            let new = previous + 1;
            if self.io_count.swap(new, Ordering::Relaxed) == previous {
                println!("io_count incremented to {}", new);
                break;
            }
        }
    }

    fn dec_io_count(&self) {
        loop {
            let previous = self.io_count.load(Ordering::Relaxed);
            let new = previous - 1;
            if self.io_count.swap(new, Ordering::Relaxed) == previous {
                println!("io_count decremented to {}", new);
                break;
            }
        }
    }
}

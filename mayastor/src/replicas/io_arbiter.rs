//! The I/O Arbiter manages I/O access to a segment.
//! An I/O is held off if a segment is already in use otherwise it is permitted.

use crate::io_bitmap::IoBitmap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct IoArbiter {
    io_count: AtomicU64,
    bitmap: IoBitmap,
}

impl IoArbiter {
    pub fn new(volume_size: u64) -> Self {
        println!("NEW IO ARBITER");
        Self {
            io_count: AtomicU64::new(0),
            bitmap: IoBitmap::new(volume_size),
        }
    }

    pub fn set_blk_size(&mut self, size: u32) {
        println!("ARBITER: set blk size to {}:{}", size, size as u64);
        self.bitmap.set_blk_size(size);
    }

    pub fn lock(&self, offset: u64, len: u64) {
        self.inc_io_count();
        self.bitmap.set(offset, len);
    }

    pub fn unlock(&self, offset: u64, len: u64) {
        self.dec_io_count();
        self.bitmap.clear(offset, len);
    }

    fn inc_io_count(&self) {
        loop {
            let previous = self.io_count.load(Ordering::Relaxed);
            let new = previous + 1;
            if self.io_count.swap(new, Ordering::Relaxed) == previous {
                break;
            }
        }
    }

    fn dec_io_count(&self) {
        loop {
            let previous = self.io_count.load(Ordering::Relaxed);
            let new = previous - 1;
            if self.io_count.swap(new, Ordering::Relaxed) == previous {
                break;
            }
        }
    }
}

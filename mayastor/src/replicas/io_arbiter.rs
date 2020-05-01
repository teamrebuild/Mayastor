//! The I/O Arbiter handles contention between front-end I/O and rebuild I/O.
//! SPDK functionality is used to ensure overlapping block ranges cannot be
//! written to at the same time.
use crate::core::{Bdev, Descriptor, IoChannel};
use futures::{channel::mpsc, StreamExt};
use spdk_sys::{bdev_lock_lba_range, bdev_unlock_lba_range, lock_range_cb};

#[derive(Debug)]
pub struct ArbiterContext {
    offset: u64,
    len: u64,
    cb_fn: lock_range_cb,
    sender: *mut mpsc::Sender<u64>,
    receiver: *mut mpsc::Receiver<u64>,
}

#[derive(Debug)]
pub struct IoArbiter {
    nexus_channel: IoChannel,
    nexus_desc: Descriptor,
}

impl IoArbiter {
    /// Create a new instance of the IoArbiter
    pub fn new(nexus_name: &str) -> Self {
        let nexus_desc = Bdev::open_by_name(&nexus_name, true)
            .expect("Failed to open Nexus bdev");
        Self {
            nexus_channel: nexus_desc.get_channel().unwrap(),
            nexus_desc,
        }
    }

    /// Lock an LBA range.
    /// The returned context MUST be used by the corresponding unlock.
    pub async fn lock(&mut self, offset: u64, len: u64) -> ArbiterContext {
        let ctx = Self::init_context(offset, len);

        unsafe {
            let rc = bdev_lock_lba_range(
                self.nexus_desc.as_ptr(),
                self.nexus_channel.as_ptr(),
                ctx.offset,
                ctx.len,
                ctx.cb_fn,
                ctx.sender as *mut _,
            );
            assert!(
                rc == 0,
                "Failed to lock LBA range, offset {}, len {}: error {}",
                ctx.offset,
                ctx.len,
                rc
            );
            trace!("Lock start: offset {}, len {}", ctx.offset, ctx.len);
            let status = (*(ctx.receiver)).next().await.unwrap();
            assert!(
                status == 0,
                "Failed to lock lba range offset {}, len {} with error {}",
                ctx.offset,
                ctx.len,
                status
            );
            trace!("Lock end: offset {}, len {}", ctx.offset, ctx.len);
        }

        ctx
    }

    /// Unlock a LBA range.
    /// The context must be the same as the one returned by lock.
    pub async fn unlock(&mut self, ctx: &mut ArbiterContext) {
        unsafe {
            let rc = bdev_unlock_lba_range(
                self.nexus_desc.as_ptr(),
                self.nexus_channel.as_ptr(),
                ctx.offset,
                ctx.len,
                ctx.cb_fn,
                ctx.sender as *mut _,
            );
            assert!(
                rc == 0,
                "Failed to unlock lba range offset {}, len {} with error {}",
                ctx.offset,
                ctx.len,
                rc
            );

            trace!("Unlock start: offset {}, len {}", ctx.offset, ctx.len);
            let status = (*(ctx.receiver)).next().await.unwrap();
            assert!(
                status == 0,
                "Failed to unlock lba range offset {}, len {} with error {}",
                ctx.offset,
                ctx.len,
                status
            );
            trace!("Unlock end: offset {}, len {}", ctx.offset, ctx.len);
        }
    }

    /// Initialise a context for use by the lock and unlock functions
    fn init_context(offset: u64, len: u64) -> ArbiterContext {
        let (s, r) = mpsc::channel::<u64>(0);
        let sender = Box::into_raw(Box::new(s));
        let receiver = Box::into_raw(Box::new(r));
        ArbiterContext {
            offset,
            len,
            cb_fn: Some(spdk_cb),
            sender,
            receiver,
        }
    }
}

extern "C" fn spdk_cb(
    ctx: *mut ::std::os::raw::c_void,
    status: ::std::os::raw::c_int,
) {
    unsafe {
        trace!("Callback status {}", status as u64);
        let s = ctx as *mut mpsc::Sender<u64>;
        if let Err(e) = (*s).start_send(status as u64) {
            panic!("Failed to send SPDK completion with error {}.", e);
        }
    }
}

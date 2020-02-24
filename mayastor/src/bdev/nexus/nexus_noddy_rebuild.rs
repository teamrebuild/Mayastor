//!
//! This file implements operations for the rebuild process
//!
//! `noddy_rebuild` does a noddy rebuild
//! 

use crate::{
    bdev::nexus::{
        nexus_bdev::{
            Nexus,
            NexusState,
        },
        nexus_channel::DREvent,
    },
};

impl Nexus {
    // rebuilds only the first bad child it finds
    pub(crate) async fn noddy_rebuild(&mut self) -> NexusState {
        let state = self.state;

        let good_child = match self.children.iter().find(|c| c.repairing == false) {
            Some(good_child) => good_child,
            None => return state,
        };

        let bad_child = match self.children.iter().find(|c| c.repairing == true) {
            Some(bad_child) => bad_child,
            None => return state,
        };

        let bdev_handle = match &bad_child.bdev_handle {
            Some(bdev_handle) => bdev_handle,
            None => return state,
        };

        let block_count = self.bdev.num_blocks();
        let block_size = self.bdev.block_len();

        info!("blocks: {}, blockSize: {}", block_count, block_size);

        let mut buf = match bdev_handle.dma_malloc(block_size as usize) {
            Ok(buf) => buf,
            Err(_) => return state,
        };

        for blk in 0..block_count {

            let addr: u64 = (blk+self.data_ent_offset)*(block_size as u64);
            if let Err(_) = good_child.read_at(addr, &mut buf).await {
                return state
            }

            if let Err(_) = bad_child.write_at(addr, &buf).await {
                return state
            }
        }

        // here we drop the immutable ref in favour a mutable ref so we can change the repair flag
        // not ideal but it works... 
        // alternatively we'd have to use something like "interior mutability" or AtomicBool
        let bad_name = bad_child.name.clone();
        let bad_child = match self.children.iter_mut().find(|c| c.name == bad_name) {
            Some(bad_child) => bad_child,
            None => return state,
        };
        
        bad_child.repairing = false;

        info!("Rebuild of child {} is complete!", bad_name);

        // child can now be part of the IO path
        self.reconfigure(DREvent::ChildOnline).await;

        self.set_state(NexusState::Online)
    }
}

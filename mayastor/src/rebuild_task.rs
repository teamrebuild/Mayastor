use crate::{
    bdev::nexus::{
        nexus_bdev::{nexus_lookup, Nexus},
        nexus_child::NexusChild,
    },
    core::{Bdev, BdevHandle, Reactors},
};
use std::convert::{TryFrom, TryInto};

#[derive(Debug)]
enum RebuildState {
    Pending,
    Running,
    Failed,
    Completed,
}

#[derive(Debug)]
pub struct RebuildTask {
    nexus_name: String,
    source: String,
    destination: String,
    block_size: u64,
    segment_size: u64, // num blocks per segment
    current_segment: u64,
    state: RebuildState,
}

impl RebuildTask {
    pub fn new(
        nexus_name: String,
        source: String,
        destination: String,
    ) -> RebuildTask {
        let s = Bdev::lookup_by_name(&source).unwrap();
        let d = Bdev::lookup_by_name(&destination).unwrap();
        if !RebuildTask::validate(&s, &d) {
            println!("Failed to validate for rebuild task");
        };

        RebuildTask {
            nexus_name,
            source,
            destination,
            block_size: s.block_len() as u64, /* validation passed, block
                                               * size same for both */
            segment_size: 20, // 10KiB segment size (512 block size)
            current_segment: 0,
            state: RebuildState::Pending,
        }
    }

    fn validate(source: &Bdev, destination: &Bdev) -> bool {
        !(source.size_in_bytes() != destination.size_in_bytes()
            || source.block_len() != destination.block_len())
    }

    fn get_bdev_handle(name: &str, read_write: bool) -> BdevHandle {
        let descriptor = Bdev::open_by_name(name, read_write).unwrap();
        BdevHandle::try_from(descriptor).unwrap()
    }

    /// get the start and end offsets of the data region for the Nexus child
    async fn get_data_offsets(child: &mut NexusChild) -> (u64, u64) {
        let label = child.probe_label().await.unwrap();
        let start = label.offset();
        let end = start + label.get_block_count();
        (start, end)
    }

    /// rebuild a non-healthy child from a healthy child
    pub async fn run(nexus_name: String) {
        let nexus = nexus_lookup(&nexus_name).unwrap();

        {
            // Sync labels
            match nexus.sync_labels().await {
                Ok(_) => println!("Synced labels"),
                Err(e) => println!("Failed to sync labels {:?}", e),
            }
        }

        let task = &mut nexus.rebuilds[0];
        task.state = RebuildState::Running;

        let mut src_child = nexus
            .children
            .iter_mut()
            .find(|c| c.name == task.source)
            .unwrap();

        let (start, end) = RebuildTask::get_data_offsets(&mut src_child).await;

        info!(
            "Src: start {} end {} block size {}",
            start, end, task.block_size
        );

        let src_hdl = RebuildTask::get_bdev_handle(&task.source, false);
        let dst_hdl = RebuildTask::get_bdev_handle(&task.destination, true);

        let mut offset = start;
        while offset < end {
            // Adjust size of the last segment
            if (offset + task.segment_size) >= start + end {
                task.segment_size = end - offset;
                info!("Adjusting segment size to {}. offset: {}, start: {}, end: {}",
                task.segment_size, offset, start, end);
            }

            let mut copy_buffer = src_hdl
                .dma_malloc(
                    (task.segment_size * task.block_size).try_into().unwrap(),
                )
                .unwrap();

            src_hdl
                .read_at(offset * task.block_size, &mut copy_buffer)
                .await
                .unwrap();

            dst_hdl
                .write_at(offset * task.block_size, &copy_buffer)
                .await
                .unwrap();
            offset += task.segment_size;
        }

        task.state = RebuildState::Completed;
        task.send_complete();
    }

    fn send_complete(&self) {
        let reactor = Reactors::current();
        let n = self.nexus_name.clone();
        reactor.send_future(async move {
            Nexus::complete_rebuild(n);
        });
    }

    pub fn print_state(nexus_name: String) {
        let nexus = nexus_lookup(&nexus_name).unwrap();
        let rebuild_task = &nexus.rebuilds[0];
        println!("Rebuild {:?}", rebuild_task.state);
    }
}

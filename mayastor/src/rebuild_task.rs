use crate::{
    bdev::nexus::{
        nexus_bdev::{nexus_lookup},
    },
    core::{Bdev, BdevHandle, Reactors, DmaBuf, DmaError},
};
use std::convert::{TryFrom, TryInto};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum RebuildError {
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Failed to validate creation parameters"))]
    InvalidParameters { },
}

#[derive(Debug)]
enum RebuildState {
    Pending,
    Running,
    #[allow(dead_code)]
    Failed,
    Completed,
}

#[derive(Debug)]
pub struct RebuildTask {
    nexus_name: String,
    source: String,
    source_hdl: BdevHandle,
    pub destination: String,
    destination_hdl: BdevHandle,
    block_size: u64,
    start: u64,
    end: u64,
    current: u64,
    segment_size_blks: u64,
    copy_buffer: DmaBuf,
    pub complete: fn(String, String) -> (),
    state: RebuildState,
}

pub struct RebuildStats {}

pub trait RebuildActions {
    fn stats(&self) -> Option<RebuildStats>;
    fn start(&self) -> ();
    fn stop(&self) -> ();
    fn pause(&self) -> ();
    fn resume(&self) -> ();
}

// todo: address unwrap errors
impl RebuildTask {
    // ideally we should move the nexus and bdev out of this and make
    // the task as generic as possible ( a simple memcpy )
    // then it should be simple to unittest
    pub async fn new(
        nexus_name: String,
        source: String,
        destination: String,
        complete: fn(String, String) -> (),
    ) -> Result<RebuildTask,RebuildError>
    {
        let source_hdl = RebuildTask::get_bdev_handle(&source, false);
        let destination_hdl = RebuildTask::get_bdev_handle(&destination, true);
        if !RebuildTask::validate(&source_hdl.get_bdev(), &destination_hdl.get_bdev()) {
            return Err(RebuildError::InvalidParameters {})
        };

        let nexus = nexus_lookup(&nexus_name).unwrap();

        let (start, end) = (nexus.data_ent_offset, nexus.bdev.num_blocks() + nexus.data_ent_offset);
        let segment_size = 10 * 1024;
        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_bdev().block_len() as u64;
        let segment_size_blks = (segment_size / block_size) as u64;

        let copy_buffer = source_hdl
            .dma_malloc(
                (segment_size_blks * block_size) as usize,
            ).context(NoCopyBuffer {})?;
        
        Ok(RebuildTask {
            nexus_name,
            source,
            source_hdl,
            destination,
            destination_hdl,
            start,
            end,
            current: start,
            block_size,
            segment_size_blks,
            copy_buffer,
            complete,
            state: RebuildState::Pending,
        })
    }

    /// rebuild a non-healthy child from a healthy child
    pub async fn run(&mut self) {
        self.state = RebuildState::Running;
        self.stats();

        while self.current < self.end {
            self.copy_one().await;
            // check if the task received a "pause/stop" request, eg child is being removed
            // alternatively we make this callback driven
        }

        self.state = RebuildState::Completed;
        self.send_complete();
    }

    async fn copy_one(&mut self) {
        // Adjust size of the last segment
        if (self.current + self.segment_size_blks) >= self.start + self.end {
            self.segment_size_blks = self.end - self.current;

            self.copy_buffer = self.source_hdl
                .dma_malloc(
                    (self.segment_size_blks * self.block_size).try_into().unwrap(),
                )
                .unwrap();

            info!("Adjusting segment size to {}. offset: {}, start: {}, end: {}",
                self.segment_size_blks, self.current, self.start, self.end);
        }

        self.source_hdl
            .read_at(self.current * self.block_size, &mut self.copy_buffer)
            .await
            .unwrap();

        self.destination_hdl
            .write_at(self.current * self.block_size, &self.copy_buffer)
            .await
            .unwrap();

        self.current += self.segment_size_blks;
    }

    fn send_complete(&self) {
        let complete = self.complete;
        complete(self.nexus_name.clone(), self.destination.clone());
    }

    pub fn print_state(&self) {
        info!("Rebuild {:?}", self.state);
    }
}

impl RebuildActions for RebuildTask {
    fn stats(&self) -> Option<RebuildStats> {
        info!(
            "State: {:?}, Src: {}, Dst: {}, start: {}, end: {}, current: {}, block: {}",
            self.state, self.source, self.destination,
            self.start, self.end, self.current, self.block_size
        );

        None
    }

    fn start(&self) {}
    fn stop(&self) {}
    fn pause(&self) {}
    fn resume(&self) {}
}

/// Helper Methods
impl RebuildTask {
    fn validate(source: &Bdev, destination: &Bdev) -> bool {
        !(source.size_in_bytes() != destination.size_in_bytes()
            || source.block_len() != destination.block_len())
    }

    fn get_bdev_handle(name: &str, read_write: bool) -> BdevHandle {
        let descriptor = Bdev::open_by_name(name, read_write).unwrap();
        BdevHandle::try_from(descriptor).unwrap()
    }

    pub fn start(nexus_name: String, task: String) {
        Reactors::current().send_future(async move {
            let nexus = nexus_lookup(&nexus_name).unwrap();
            let task = nexus.rebuilds.iter_mut().find(|t| t.destination == task).unwrap();
            task.run().await;
            task.print_state();
        });
    }
}
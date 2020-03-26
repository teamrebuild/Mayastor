use crate::core::{Bdev, BdevHandle, CoreError, DmaBuf, DmaError, Reactors};
use crossbeam::channel::{unbounded, Receiver, Sender};
use once_cell::sync::OnceCell;
use snafu::{ResultExt, Snafu};
use spdk_sys::spdk_get_thread;
use std::{cell::UnsafeCell, fmt};

pub struct RebuildInstances {
    inner: UnsafeCell<Vec<RebuildTask>>,
}

unsafe impl Sync for RebuildInstances {}
unsafe impl Send for RebuildInstances {}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum RebuildError {
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Failed to validate rebuild task creation parameters"))]
    InvalidParameters {},
    #[snafu(display("Failed to get a handle for bdev {}", bdev))]
    NoBdevHandle { source: CoreError, bdev: String },
    #[snafu(display("IO failed for bdev {}", bdev))]
    IoError { source: CoreError, bdev: String },
    #[snafu(display("Failed to find rebuild task {}", task))]
    TaskNotFound { task: String },
    #[snafu(display("Task {} already exists", task))]
    TaskAlreadyExists { task: String },
    #[snafu(display("Missing rebuild destination {}", task))]
    MissingDestination { task: String },
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RebuildState {
    Pending,
    Running,
    Stopped,
    Failed,
    Completed,
}

impl fmt::Display for RebuildState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RebuildState::Pending => write!(f, "pending"),
            RebuildState::Running => write!(f, "running"),
            RebuildState::Stopped => write!(f, "stopped"),
            RebuildState::Failed => write!(f, "failed"),
            RebuildState::Completed => write!(f, "completed"),
        }
    }
}

//#[derive(Debug)]
pub struct RebuildTask {
    pub nexus: String,
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
    complete_fn: fn(String, String) -> (),
    pub complete_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    pub state: RebuildState,
}

pub struct RebuildStats {}

pub trait RebuildActions {
    fn stats(&self) -> Option<RebuildStats>;
    fn start(&mut self) -> Receiver<RebuildState>;
    fn stop(&mut self);
    fn pause(&mut self);
    fn resume(&mut self);
}

impl fmt::Debug for RebuildTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Hi")
    }
}

impl RebuildTask {
    pub fn create<'b>(
        nexus: &str,
        source: &str,
        destination: &'b str,
        start: u64,
        end: u64,
        complete_fn: fn(String, String) -> (),
    ) -> Result<&'b mut Self, RebuildError> {
        Self::new(nexus, source, destination, start, end, complete_fn)?
            .store()?;

        Ok(Self::lookup(destination)?)
    }

    pub fn new(
        nexus: &str,
        source: &str,
        destination: &str,
        start: u64,
        end: u64,
        complete_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        let source_hdl =
            BdevHandle::open(source, false, false).context(NoBdevHandle {
                bdev: source,
            })?;
        let destination_hdl = BdevHandle::open(destination, true, false)
            .context(NoBdevHandle {
                bdev: destination,
            })?;

        if !Self::validate(&source_hdl.get_bdev(), &destination_hdl.get_bdev())
        {
            return Err(RebuildError::InvalidParameters {});
        };

        let segment_size = 10 * 1024;
        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_bdev().block_len() as u64;
        let segment_size_blks = (segment_size / block_size) as u64;

        let copy_buffer = source_hdl
            .dma_malloc((segment_size_blks * block_size) as usize)
            .context(NoCopyBuffer {})?;

        let (source, destination) =
            (source.to_string(), destination.to_string());
        let nexus = nexus.to_string();

        Ok(Self {
            nexus,
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
            complete_fn,
            complete_chan: unbounded::<RebuildState>(),
            state: RebuildState::Pending,
        })
    }

    /// Lookup a task by its name destination uri
    pub fn lookup(name: &str) -> Result<&mut Self, RebuildError> {
        if let Some(task) = Self::get_instances()
            .iter_mut()
            .find(|n| n.destination == name)
        {
            Ok(task)
        } else {
            Err(RebuildError::TaskNotFound {
                task: name.to_owned(),
            })
        }
    }

    pub fn count() -> usize {
        Self::get_instances().len()
    }

    /// Lookup and remove task by its destination uri
    pub fn remove(name: &str) -> Result<Self, RebuildError> {
        match Self::get_instances()
            .iter()
            .position(|t| t.destination == name)
        {
            Some(task_index) => Ok(Self::get_instances().remove(task_index)),
            None => Err(RebuildError::TaskNotFound {
                task: name.to_owned(),
            }),
        }
    }

    fn store(self: Self) -> Result<(), RebuildError> {
        let rebuild_list = Self::get_instances();

        if rebuild_list
            .iter()
            .any(|n| n.destination == self.destination)
        {
            Err(RebuildError::TaskAlreadyExists {
                task: self.destination,
            })
        } else {
            rebuild_list.push(self);
            Ok(())
        }
    }

    /// rebuild a non-healthy child from a healthy child from start to end
    async fn run(&mut self) {
        self.state = RebuildState::Running;
        self.current = self.start;
        self.stats();

        while self.current < self.end {
            if let Err(e) = self.copy_one().await {
                error!("Failed to copy segment {}", e);
                self.state = RebuildState::Failed;
                self.send_complete();
            }
            // TODO: check if the task received a "pause" request, eg suspend
            // rebuild
            if self.state == RebuildState::Stopped {
                return self.send_complete();
            }
        }

        self.state = RebuildState::Completed;
        self.send_complete();
    }

    /// copy one segment worth of data from source into destination
    async fn copy_one(&mut self) -> Result<(), RebuildError> {
        // Adjust size of the last segment
        if (self.current + self.segment_size_blks) >= self.start + self.end {
            self.segment_size_blks = self.end - self.current;

            self.copy_buffer = self
                .source_hdl
                .dma_malloc((self.segment_size_blks * self.block_size) as usize)
                .context(NoCopyBuffer {})?;

            info!(
                "Adjusting segment size to {}. offset: {}, start: {}, end: {}",
                self.segment_size_blks, self.current, self.start, self.end
            );
        }

        self.source_hdl
            .read_at(self.current * self.block_size, &mut self.copy_buffer)
            .await
            .context(IoError {
                bdev: &self.source,
            })?;

        self.destination_hdl
            .write_at(self.current * self.block_size, &self.copy_buffer)
            .await
            .context(IoError {
                bdev: &self.destination,
            })?;

        self.current += self.segment_size_blks;
        Ok(())
    }

    fn send_complete(&mut self) {
        self.stats();
        (self.complete_fn)(self.nexus.clone(), self.destination.clone());
        if let Err(e) = self.complete_chan.0.send(self.state) {
            error!("Rebuild Task {} of nexus {} failed to send complete via the unbound channel with err {}", self.destination, self.nexus, e);
        }
    }

    fn validate(source: &Bdev, destination: &Bdev) -> bool {
        !(source.size_in_bytes() != destination.size_in_bytes()
            || source.block_len() != destination.block_len())
    }

    /// Changing the state should be performed on the same
    /// reactor as the rebuild task
    fn change_state(&mut self, new_state: RebuildState) {
        info!(
            "Rebuild task {}: changing state from {:?} to {:?}",
            self.destination, self.state, new_state
        );
        self.state = new_state;
    }

    /// return instances, we ensure that this can only ever be called on a
    /// properly allocated thread
    pub fn get_instances() -> &'static mut Vec<Self> {
        let thread = unsafe { spdk_get_thread() };
        if thread.is_null() {
            panic!("not called from SPDK thread")
        }

        static REBUILD_INSTANCES: OnceCell<RebuildInstances> = OnceCell::new();

        let global_instances =
            REBUILD_INSTANCES.get_or_init(|| RebuildInstances {
                inner: UnsafeCell::new(Vec::new()),
            });

        unsafe { &mut *global_instances.inner.get() }
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

    // todo: ideally we'd want the nexus out of here but sadly rust does not yet
    // support async trait's
    // the course of action might just be not using traits
    fn start(&mut self) -> Receiver<RebuildState> {
        let destination = self.destination.clone();
        let complete_receiver = self.complete_chan.clone().1;

        Reactors::current().send_future(async move {
            let task = match RebuildTask::lookup(&destination) {
                Ok(task) => task,
                Err(_) => {
                    return error!(
                        "Failed to find the rebuild task {}",
                        destination
                    );
                }
            };

            task.run().await;
        });
        complete_receiver
    }
    fn stop(&mut self) {
        self.change_state(RebuildState::Stopped);
    }
    fn pause(&mut self) {
        todo!("pause the rebuild task");
    }
    fn resume(&mut self) {
        todo!("resume the rebuild task");
    }
}

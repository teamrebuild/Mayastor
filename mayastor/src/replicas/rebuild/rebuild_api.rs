#![warn(missing_docs)]

use crate::core::{BdevHandle, CoreError, DmaError};
use crossbeam::channel::{Receiver, Sender};
use snafu::Snafu;
use std::fmt;

use super::rebuild_impl::*;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
#[allow(missing_docs)]
/// Various rebuild errors when interacting with a rebuild job or
/// encountered during a rebuild copy
pub enum RebuildError {
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Failed to validate rebuild job creation parameters"))]
    InvalidParameters {},
    #[snafu(display("Failed to get a handle for bdev {}", bdev))]
    NoBdevHandle { source: CoreError, bdev: String },
    #[snafu(display("IO failed for bdev {}", bdev))]
    IoError { source: CoreError, bdev: String },
    #[snafu(display("Failed to find rebuild job {}", job))]
    JobNotFound { job: String },
    #[snafu(display("Job {} already exists", job))]
    JobAlreadyExists { job: String },
    #[snafu(display("Missing rebuild destination {}", job))]
    MissingDestination { job: String },
    #[snafu(display(
        "{} operation failed because current rebuild state is {}.",
        operation,
        state,
    ))]
    OpError { operation: String, state: String },
}

#[derive(Debug, PartialEq, Copy, Clone)]
/// allowed states for a rebuild job
pub enum RebuildState {
    /// Pending when the job is newly created
    Pending,
    /// Running when the job is rebuilding
    Running,
    /// Stopped when the job is halted as requested through stop
    /// and pending its removal
    Stopped,
    /// Paused when the job is paused as requested through pause
    Paused,
    /// Failed when an IO (R/W) operation was failed
    /// there are no retries as it currently stands
    Failed,
    /// Completed when the rebuild was sucessfully completed
    Completed,
}

impl fmt::Display for RebuildState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RebuildState::Pending => write!(f, "pending"),
            RebuildState::Running => write!(f, "running"),
            RebuildState::Stopped => write!(f, "stopped"),
            RebuildState::Paused => write!(f, "paused"),
            RebuildState::Failed => write!(f, "failed"),
            RebuildState::Completed => write!(f, "completed"),
        }
    }
}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end
pub struct RebuildJob {
    /// name of the nexus associated with the rebuild job
    pub nexus: String,
    /// source URI of the healthy child to rebuild from
    pub(super) source: String,
    pub(super) source_hdl: BdevHandle,
    /// target URI of the out of sync child in need of a rebuild
    pub destination: String,
    pub(super) destination_hdl: BdevHandle,
    pub(super) block_size: u64,
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) next: u64,
    pub(super) segment_size_blks: u64,
    pub(super) tasks: RebuildTasks,
    pub(super) complete_fn: fn(String, String) -> (),
    /// channel used to signal rebuild completion
    pub complete_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    /// current state of the rebuild job
    pub state: RebuildState,
}

/// Place holder for rebuild statistics
pub struct RebuildStats {}

/// Public facing operations on a Rebuild Job
pub trait RebuildOperations {
    /// Collects statistics from the job
    fn stats(&self) -> Option<RebuildStats>;
    /// Schedules the job to start in a future and returns a complete channel
    /// which can be waited on
    fn start(&mut self) -> Receiver<RebuildState>;
    /// Stops the job which then triggers the completion hooks
    fn stop(&mut self) -> Result<(), RebuildError>;
    /// pauses the job which can then be later resumed
    fn pause(&mut self) -> Result<(), RebuildError>;
    /// Resumes a previously paused job
    /// this could be used to mitigate excess load on the source bdev, eg
    /// too much contention with frontend IO
    fn resume(&mut self) -> Result<(), RebuildError>;
}

impl RebuildJob {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end; complete_fn callback is called when the rebuild
    /// completes with the nexus and destinarion URI as arguments
    pub fn create<'a>(
        nexus: &str,
        source: &str,
        destination: &'a str,
        start: u64,
        end: u64,
        complete_fn: fn(String, String) -> (),
    ) -> Result<&'a mut Self, RebuildError> {
        Self::new(nexus, source, destination, start, end, complete_fn)?
            .store()?;

        Ok(Self::lookup(destination)?)
    }

    /// Lookup a rebuild job by its destination uri and return it
    pub fn lookup(name: &str) -> Result<&mut Self, RebuildError> {
        if let Some(job) = Self::get_instances().get_mut(name) {
            Ok(job)
        } else {
            Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            })
        }
    }

    /// Lookup all rebuilds jobs with name as its source
    pub fn lookup_src(name: &str) -> Vec<&mut Self> {
        let mut jobs = Vec::new();

        Self::get_instances()
            .iter_mut()
            .filter(|j| j.1.source == name)
            .for_each(|j| jobs.push(j.1));

        jobs
    }

    /// Lookup a rebuild job by its destination uri then remove and return it
    pub fn remove(name: &str) -> Result<Self, RebuildError> {
        match Self::get_instances().remove(name) {
            Some(job) => Ok(job),
            None => Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            }),
        }
    }

    /// Number of rebuild job instances
    pub fn count() -> usize {
        Self::get_instances().len()
    }
}

use crossbeam::channel::unbounded;

pub mod common;

use mayastor::{
    bdev::{nexus_lookup, VerboseError},
    core::{MayastorCliArgs, MayastorEnvironment, Reactor},
    replicas::rebuild::{RebuildJob, RebuildState, SEGMENT_SIZE},
};

use once_cell::sync::Lazy;
use rpc::mayastor::ShareProtocolNexus;
use std::sync::Mutex;

// each test `should` use a different nexus name to prevent clashing with
// one another. This allows the failed tests to `panic gracefully` improving
// the output log and allowing the CI to fail gracefully as well
static NEXUS_NAME: Lazy<Mutex<&str>> = Lazy::new(|| Mutex::new("Default"));
pub fn nexus_name() -> &'static str {
    &NEXUS_NAME.lock().unwrap()
}

static NEXUS_SIZE: u64 = 5 * 1024 * 1024; // 10MiB

// approximate on-disk metadata that will be written to the child by the nexus
const META_SIZE: u64 = 5 * 1024 * 1024; // 5MiB
const MAX_CHILDREN: u64 = 16;

fn test_ini(name: &'static str) {
    *NEXUS_NAME.lock().unwrap() = name;

    test_init!();
    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE + META_SIZE);
    }
}
fn test_fini() {
    //mayastor_env_stop(0);
    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
    }
}

fn get_disk(number: u64) -> String {
    format!("/tmp/disk{}.img", number)
}
fn get_dev(number: u64) -> String {
    format!("aio://{}?blk_size=512", get_disk(number))
}

#[test]
fn rebuild_test_basic() {
    test_ini("rebuild_test_basic");

    Reactor::block_on(async {
        nexus_create(1).await;
        nexus_add_child(1, true).await;
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
// test the rebuild flag of the add_child operation
fn rebuild_test_add() {
    test_ini("rebuild_test_add");

    Reactor::block_on(async {
        nexus_create(1).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();

        nexus.add_child(&get_dev(1), true).await.unwrap();
        nexus
            .start_rebuild(&get_dev(1))
            .expect_err("rebuild expected to be present");
        nexus_test_child(1).await;

        nexus.add_child(&get_dev(2), false).await.unwrap();
        let _ = nexus
            .start_rebuild(&get_dev(2))
            .expect("rebuild not expected to be present");

        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_progress() {
    test_ini("rebuild_progress");

    async fn test_progress(polls: u64, progress: u64) -> u64 {
        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.resume_rebuild(&get_dev(1)).await.unwrap();
        // { polls } to poll with an expr rather than an ident
        reactor_poll!({ polls });
        nexus.pause_rebuild(&get_dev(1)).await.unwrap();
        let p = nexus.get_rebuild_progress(&get_dev(1)).unwrap();
        assert!(p.progress > progress);
        p.progress
    };

    Reactor::block_on(async {
        nexus_create(1).await;
        nexus_add_child(1, false).await;
        // naive check to see if progress is being made
        let mut progress = 0;
        for _ in 0 .. 10 {
            progress = test_progress(50, progress).await;
        }
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_child_faulted() {
    test_ini("rebuild_child_faulted");

    Reactor::block_on(async move {
        nexus_create(2).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus
            .start_rebuild(&get_dev(1))
            .expect_err("Rebuild only degraded children!");

        nexus.remove_child(&get_dev(1)).await.unwrap();
        assert_eq!(nexus.children.len(), 1);
        nexus
            .start_rebuild(&get_dev(0))
            .expect_err("Cannot rebuild from the same child");

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_dst_removal() {
    test_ini("rebuild_dst_removal");

    Reactor::block_on(async move {
        let new_child = 2;
        nexus_create(new_child).await;
        nexus_add_child(new_child, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.pause_rebuild(&get_dev(new_child)).await.unwrap();
        nexus.remove_child(&get_dev(new_child)).await.unwrap();

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_src_removal() {
    test_ini("rebuild_src_removal");

    Reactor::block_on(async move {
        let new_child = 2;
        assert!(new_child > 1);
        nexus_create(new_child).await;
        nexus_add_child(new_child, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.pause_rebuild(&get_dev(new_child)).await.unwrap();
        nexus.remove_child(&get_dev(0)).await.unwrap();

        // tests if new_child which had it's original rebuild src removed
        // ended up being rebuilt successfully
        nexus_test_child(new_child).await;

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

async fn nexus_create(children: u64) {
    nexus_create_with_size(NEXUS_SIZE, children).await
}
async fn nexus_create_with_size(size: u64, children: u64) {
    let mut ch = Vec::new();
    for i in 0 .. children {
        ch.push(get_dev(i));
    }

    mayastor::bdev::nexus_create(nexus_name(), size, None, &ch)
        .await
        .unwrap();

    let nexus = nexus_lookup(nexus_name()).unwrap();
    let device = common::device_path_from_uri(
        nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap(),
    );

    let nexus_device = device.clone();
    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::dd_urandom_blkdev(&nexus_device))
    });
    reactor_poll!(r);

    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::compare_nexus_device(&device, &get_disk(0), true))
    });
    reactor_poll!(r);
}

async fn nexus_add_child(new_child: u64, wait: bool) {
    let nexus = nexus_lookup(nexus_name()).unwrap();

    nexus.add_child(&get_dev(new_child), true).await.unwrap();

    if wait {
        common::wait_for_rebuild(
            get_dev(new_child),
            RebuildState::Completed,
            std::time::Duration::from_secs(10),
        )
        .unwrap();

        nexus_test_child(new_child).await;
    } else {
        // allows for the rebuild to start running (future run by the reactor)
        reactor_poll!(2);
    }
}

async fn nexus_test_child(child: u64) {
    common::wait_for_rebuild(
        get_dev(child),
        RebuildState::Completed,
        std::time::Duration::from_secs(10),
    )
    .unwrap();

    let nexus = nexus_lookup(nexus_name()).unwrap();

    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::compare_devices(
            &get_disk(0),
            &get_disk(child),
            nexus.size(),
            true,
        ))
    });
    reactor_poll!(r);
}

#[test]
// test rebuild with different combinations of sizes for src and dst children
fn rebuild_sizes() {
    test_ini("rebuild_sizes");

    let nexus_size = 10 * 1024 * 1024; // 10MiB
    let child_size = nexus_size + META_SIZE;
    let mut test_cases = vec![
        (nexus_size, child_size, child_size),
        (nexus_size, child_size * 2, child_size),
        (nexus_size, child_size, child_size * 2),
        (nexus_size, child_size * 2, child_size * 2),
    ];
    // now for completeness sake we also the cases where the actual
    // nexus_size will be lower due to the on-disk metadata
    let child_size = nexus_size;
    test_cases.extend(vec![
        (nexus_size, child_size, child_size),
        (nexus_size, child_size * 2, nexus_size),
        (nexus_size, child_size, child_size * 2),
        (nexus_size, child_size * 2, child_size * 2),
    ]);

    for (test_case_index, test_case) in test_cases.iter().enumerate() {
        common::delete_file(&[get_disk(0), get_disk(1), get_disk(1)]);
        // first healthy child in the list is used as the rebuild source
        common::truncate_file_bytes(&get_disk(0), test_case.1);
        common::truncate_file_bytes(&get_disk(1), test_case.0);
        common::truncate_file_bytes(&get_disk(2), test_case.2);

        let nexus_size = test_case.0;
        Reactor::block_on(async move {
            // add an extra child so that the minimum size is set to
            // match the nexus size
            nexus_create_with_size(nexus_size, 2).await;
            let nexus = nexus_lookup(nexus_name()).unwrap();
            nexus.add_child(&get_dev(2), false).await.unwrap();
            // within start_rebuild the size should be validated
            let _ = nexus.start_rebuild(&get_dev(2)).unwrap_or_else(|e| {
                log::error!( "Case {} - Child should have started to rebuild but got error:\n {:}",
                    test_case_index, e.verbose());
                panic!(
                    "Case {} - Child should have started to rebuild but got error:\n {}",
                    test_case_index, e.verbose()
                )
            });
            // sanity check that the rebuild does succeed
            nexus_test_child(2).await;

            nexus.destroy().await.unwrap();
        });
    }

    test_fini();
}

#[test]
// tests the rebuild with multiple size and a non-multiple size of the segment
fn rebuild_segment_sizes() {
    test_ini("rebuild_segment_sizes");

    assert!(SEGMENT_SIZE > 512 && SEGMENT_SIZE < NEXUS_SIZE);

    let test_cases = vec![
        // multiple of SEGMENT_SIZE
        SEGMENT_SIZE * 10,
        // not multiple of SEGMENT_SIZE
        (SEGMENT_SIZE * 10) + 512,
    ];

    for test_case in test_cases.iter() {
        let nexus_size = *test_case;
        Reactor::block_on(async move {
            nexus_create_with_size(nexus_size, 1).await;
            nexus_add_child(1, true).await;
            nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
        });
    }

    test_fini();
}

#[test]
fn rebuild_olookup() {
    test_ini("rebuild_lookup");

    Reactor::block_on(async move {
        let children = 6;
        nexus_create(children).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.add_child(&get_dev(children), false).await.unwrap();

        for child in 0 .. children {
            RebuildJob::lookup(&get_dev(child)).expect_err("Should not exist");

            RebuildJob::lookup_src(&get_dev(child))
                .iter()
                .inspect(|&job| {
                    log::error!(
                        "Job {:?} should be associated with src child {}",
                        job,
                        child
                    );
                })
                .any(|_| panic!("Should not have found any jobs!"));
        }

        let _ = nexus.start_rebuild(&get_dev(children)).unwrap();
        for child in 0 .. children - 1 {
            RebuildJob::lookup(&get_dev(child))
                .expect_err("rebuild job not created yet");
        }
        let src = RebuildJob::lookup(&get_dev(children))
            .expect("now the job should exist")
            .source
            .clone();

        for child in 0 .. children {
            if get_dev(child) != src {
                RebuildJob::lookup_src(&get_dev(child))
                    .iter()
                    .filter(|s| s.destination != get_dev(child))
                    .inspect(|&job| {
                        log::error!(
                            "Job {:?} should be associated with src child {}",
                            job,
                            child
                        );
                    })
                    .any(|_| panic!("Should not have found any jobs!"));
            }
        }

        assert_eq!(
            RebuildJob::lookup_src(&src)
                .iter()
                .inspect(|&job| {
                    assert_eq!(job.destination, get_dev(children));
                })
                .count(),
            1
        );
        nexus
            .add_child(&get_dev(children + 1), false)
            .await
            .unwrap();
        let _ = nexus.start_rebuild(&get_dev(children + 1)).unwrap();
        assert_eq!(RebuildJob::lookup_src(&src).len(), 2);

        nexus.remove_child(&get_dev(children)).await.unwrap();
        nexus.remove_child(&get_dev(children + 1)).await.unwrap();
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
// todo: decide whether to keep the idempotence on the operations or to
// create a RPC version which achieves the idempotence
fn rebuild_operations() {
    test_ini("rebuild_operations");

    Reactor::block_on(async {
        nexus_create(1).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();

        nexus
            .resume_rebuild(&get_dev(1))
            .await
            .expect_err("no rebuild to resume");

        nexus_add_child(1, false).await;

        nexus
            .resume_rebuild(&get_dev(1))
            .await
            .expect("already running");

        nexus.pause_rebuild(&get_dev(1)).await.unwrap();
        reactor_poll!(10);
        // already pausing so no problem
        nexus.pause_rebuild(&get_dev(1)).await.unwrap();

        let _ = nexus
            .start_rebuild(&get_dev(1))
            .expect_err("a rebuild already exists");

        nexus.stop_rebuild(&get_dev(1)).await.unwrap();
        common::wait_for_rebuild(
            get_dev(1),
            RebuildState::Stopped,
            // already stopping, should be enough
            std::time::Duration::from_millis(250),
        )
        .unwrap();
        // already stopped
        nexus.stop_rebuild(&get_dev(1)).await.unwrap();

        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_concurrently() {
    test_ini("rebuild_concurrently");

    let concurrent_rebuilds = 4;
    Reactor::block_on(async move {
        nexus_create(1).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();

        for child in 1 .. concurrent_rebuilds {
            nexus_add_child(child, false).await;
        }

        for child in 1 .. concurrent_rebuilds {
            common::wait_for_rebuild(
                get_dev(child),
                RebuildState::Completed,
                std::time::Duration::from_secs(10),
            )
            .unwrap();
            nexus.remove_child(&get_dev(child)).await.unwrap();
        }

        for child in 1 .. concurrent_rebuilds {
            nexus_add_child(child, false).await;
        }

        for child in 1 .. concurrent_rebuilds {
            common::wait_for_rebuild(
                get_dev(child),
                RebuildState::Running,
                std::time::Duration::from_millis(100),
            )
            .unwrap();
            nexus.remove_child(&get_dev(child)).await.unwrap();
        }

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

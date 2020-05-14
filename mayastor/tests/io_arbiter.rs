#[macro_use]
extern crate log;

pub mod common;

use crossbeam::channel::{after, select, unbounded, Receiver, Sender};
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        Bdev,
        DmaBuf,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
        Reactors,
    },
    replicas::rebuild::io_arbiter::IoArbiter,
};
use std::{thread::sleep, time::Duration};

const NEXUS_NAME: &str = "arbiter_nexus";
const NEXUS_SIZE: u64 = 10 * 1024 * 1024;
const NUM_NEXUS_CHILDREN: u64 = 2;

fn test_ini() {
    test_init!();
    for i in 0 .. NUM_NEXUS_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE);
    }

    Reactor::block_on(async {
        create_nexus().await;
    });
}

macro_rules! init_multiple_reactors {
    () => {
        common::MSTEST.get_or_init(|| {
            common::mayastor_test_init();
            MayastorEnvironment::new(MayastorCliArgs {
                reactor_mask: "0x3".to_string(),
                ..Default::default()
            })
            .init()
        });
    };
}

fn test_init_multiple_reactors() {
    init_multiple_reactors!();
    for i in 0 .. NUM_NEXUS_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE);
    }

    Reactor::block_on(async {
        create_nexus().await;
    });
}

fn test_fini() {
    for i in 0 .. NUM_NEXUS_CHILDREN {
        common::delete_file(&[get_disk(i)]);
    }

    Reactor::block_on(async move {
        let nexus = nexus_lookup(NEXUS_NAME).unwrap();
        nexus.destroy().await.unwrap();
    });
}

fn get_disk(number: u64) -> String {
    format!("/tmp/disk{}.img", number)
}

fn get_dev(number: u64) -> String {
    format!("aio://{}?blk_size=512", get_disk(number))
}

async fn create_nexus() {
    let mut ch = Vec::new();
    for i in 0 .. NUM_NEXUS_CHILDREN {
        ch.push(get_dev(i));
    }

    match nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &ch).await {
        Ok(_) => println!("Created nexus"),
        Err(e) => println!("Failed to create nexus: {}", e),
    }
}

// Sets each of the reactors to the running state so they are available for use.
// This function doesn't run the reactors, that has to be done by calling "poll"
// outside of this.
fn init_reactors() -> Vec<&'static Reactor> {
    let mut reactors = Vec::new();
    for r in Reactors::iter() {
        reactors.push(r);
        r.running();
    }

    reactors
}

// Function should be called after an expected timeout in the test.
// This sends a signal to clean up and waits for the test to complete.
// The clean up logic needs to be implemented in the individual tests.
fn cleanup_after_timeout(s: Sender<()>, r: Receiver<()>) {
    trace!("Send cleanup message");
    let _ = s.send(());
    reactor_poll!(100);

    // Expect cleanup to succeed
    select! {
        recv(r) -> _ => println!("Cleanup succeeded"),
        recv(after(Duration::from_secs(1))) -> _ => panic!("Cleanup timed
    out"), }
}

#[test]
// Test acquiring and releasing a lock.
fn lock_unlock() {
    let (s, r) = unbounded::<()>();

    let _ = std::thread::spawn(move || {
        test_ini();

        let reactor = Reactors::current();
        reactor.running();
        reactor.send_future(async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 5).await;
            arbiter.unlock(&mut ctx).await;
            let _ = s.send(());
        });
        reactor_poll!(100);

        select! {
            recv(r) -> _ => trace!("Success"),
            recv(after(Duration::from_secs(1))) -> _ => panic!("Timed out"),
        }

        test_fini();
    })
    .join();
}

#[test]
// Test making multiple request for a lock for an overlapping LBA range.
// Unlocking is not performed therefore only the first lock request should
// succeed.
fn multiple_locks() {
    let (s, r) = unbounded::<()>();
    let (cleanup_s, cleanup_r) = unbounded::<()>();

    let _ = std::thread::spawn(move || {
        test_init_multiple_reactors();

        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 10).await;

            // Cleanup after test finished
            let _ = cleanup_r.recv();
            trace!("Received cleanup message");
            arbiter.unlock(&mut ctx).await;
        };

        let fut2 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 5).await;

            arbiter.unlock(&mut ctx).await;
            let _ = s.send(());
        };

        let reactors = init_reactors();
        assert_eq!(reactors.len(), 2);

        reactors[0].send_future(fut1);
        reactors[0].poll_once();

        // Reactor 1 is polling by default
        reactors[1].send_future(fut2);

        // Expect a timeout - the first lock request is never unlocked, so the
        // second lock request should never succeed.
        select! {
            recv(r) -> _ => panic!("Shouldn't succeed on double lock"),
            recv(after(Duration::from_secs(1))) -> _ => println!("Success"),
        }

        cleanup_after_timeout(cleanup_s, r);
        test_fini();
    })
    .join();
}

#[test]
// Test multiple lock requests for an overlapping LBA range with unlocking.
// The first lock is acquired and is only unlocked after the timer times out.
// Whilst the first lock is held, the second lock request should be blocked.
// The second lock request should only succeed after the first lock is unlocked.
fn multiple_locks_with_unlocks() {
    let (s, r) = unbounded::<()>();

    let _ = std::thread::spawn(move || {
        test_init_multiple_reactors();

        // Acquire lock and hold it for an extended duration.
        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);

            trace!("fut1: REQUEST LOCK");
            let mut ctx = arbiter.lock(1, 10).await;
            trace!("fut1: ACQUIRED LOCK");

            // Wait for timer to expire before unlocking
            //let _ = timer_receiver.recv();
            sleep(Duration::from_secs(1));
            arbiter.unlock(&mut ctx).await;
            trace!("fut1: RELEASED LOCK");
        };

        // Attempt to acquire the lock
        let fut2 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);

            trace!("fut2: REQUEST LOCK");
            // The lock should not succeed until the first lock is unlocked
            let mut ctx = arbiter.lock(1, 5).await;
            trace!("fut2: ACQUIRED LOCK");

            arbiter.unlock(&mut ctx).await;
            trace!("f2: RELEASED LOCK");

            let _ = s.send(());
        };

        let reactors = init_reactors();
        assert_eq!(reactors.len(), 2);

        reactors[0].send_future(fut1);
        reactors[0].poll_once();

        reactors[1].send_future(fut2);

        reactor_poll!(1000);

        // The second lock request should succeed and unlock once the first lock
        // is unlocked.
        select! {
            recv(r) -> _ => trace!("Success"),
            recv(after(Duration::from_secs(5))) -> _ => panic!("Timed out"),
        }

        test_fini();
    })
    .join();
}

#[test]
// Test issuing a front-end I/O to a LBA range that overlaps a locked range.
// The front-end I/O should never complete because the lock is neverunlocked.
fn fe_io_lock_held() {
    let (s, r) = unbounded::<()>();
    let (s1, r1) = unbounded::<()>();
    let (cleanup_s, cleanup_r) = unbounded::<()>();

    let _ = std::thread::spawn(move || {
        test_init_multiple_reactors();

        // Acquire the lock
        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 5).await;
            trace!("fut1: LOCKED");
            let _ = s1.send(());

            // Cleanup
            let _ = cleanup_r.recv();
            arbiter.unlock(&mut ctx).await;
        };

        // Issue front-end write I/O
        let fut2 = async move {
            let nexus_desc = Bdev::open_by_name(&NEXUS_NAME, true).unwrap();
            let h = nexus_desc.into_handle().unwrap();

            let blk = 2;
            let blk_size = 512;
            let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

            trace!("fut2: running");

            // Wait for fut1 to obtain lock
            let _ = r1.recv();

            trace!("fut2: write to nexus");
            match h.write_at((blk * blk_size) as u64, &buf).await {
                Ok(_) => trace!("Successfully wrote to nexus"),
                Err(e) => trace!("Failed to write to nexus: {}", e),
            }

            let _ = s.send(());
        };

        let reactors = init_reactors();
        assert_eq!(reactors.len(), 2);

        reactors[0].send_future(fut1);
        reactors[0].poll_once();

        reactors[1].send_future(fut2);

        // Expect a timeout - the nexus shouldn't be able to be written to while
        // a lock is held on an overlapping LBA range
        select! {
            recv(r) -> _ => panic!("Shouldn't be able to write to nexus"),
            recv(after(Duration::from_secs(1))) -> _ => println!("Success"),
        };

        cleanup_after_timeout(cleanup_s, r);
        test_fini();
    })
    .join();
}

#[test]
// Test issuing a front-end I/O to a LBA range that overlaps a locked range but
// which is subsequently unlocked.
// The front-end I/O should eventually complete after the lock is unlocked.
fn fe_io_lock_unlock() {
    let (s, r) = unbounded::<()>();
    let (s1, r1) = unbounded::<()>();

    let _ = std::thread::spawn(move || {
        test_init_multiple_reactors();

        // Acquire the lock and unlock after delay
        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 5).await;
            info!("fut1: LOCKED");
            let _ = s1.send(());

            // Wait for timer to expire before unlocking
            //let _ = timer_receiver.recv();
            sleep(Duration::from_secs(1));
            arbiter.unlock(&mut ctx).await;
            info!("fut1: UNLOCKED");
        };

        // Issue a write I/O to the nexus
        let fut2 = async move {
            let nexus_desc = Bdev::open_by_name(&NEXUS_NAME, true).unwrap();
            let h = nexus_desc.into_handle().unwrap();

            let blk = 2;
            let blk_size = 512;
            let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

            // Wait for f1 to obtain lock
            let _ = r1.recv();

            info!("fut2: write to nexus");
            match h.write_at((blk * blk_size) as u64, &buf).await {
                Ok(_) => info!("Successfully wrote to nexus"),
                Err(e) => info!("Failed to write to nexus: {}", e),
            }

            let _ = s.send(());
        };

        let reactors = init_reactors();
        assert_eq!(reactors.len(), 2);

        reactors[0].send_future(fut1);
        reactors[0].poll_once();

        reactors[1].send_future(fut2);

        reactor_poll!(100);

        // The write to the nexus should complete once the first lock is
        // unlocked
        select! {
            recv(r) -> _ => println!("Success"),
            recv(after(Duration::from_secs(5))) -> _ => panic!("Expected front end write to succeed"),
        }

        test_fini();
    }).join();
}

#[test]
// Test issuing a front-end I/O which acquires a lock over the LBA range and
// then attempt to request a lock for an overlapping range. The front-end I/O
// then releases the lock.
fn fe_io_holding_lock_unlock() {
    let (s, r) = unbounded::<()>();

    let _ = std::thread::spawn(move || {
        test_init_multiple_reactors();

        // Future 1, issue write I/O to nexus
        let fut1 = async move {
            let nexus_desc = Bdev::open_by_name(&NEXUS_NAME, true).unwrap();
            let h = nexus_desc.into_handle().unwrap();

            let blk = 2;
            let blk_size = 512;
            let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

            trace!("fut1: write to nexus");
            match h.write_at((blk * blk_size) as u64, &buf).await {
                Ok(_) => trace!("Successfully wrote to nexus"),
                Err(e) => trace!("Failed to write to nexus: {}", e),
            }
        };

        // Future 2, request lock
        let fut2 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            trace!("fut2: REQUEST LOCK");
            arbiter.lock(1, 5).await;
            trace!("fut2: LOCKED");
            let _ = s.send(());
        };

        // Configure the reactor
        Reactors::init();
        let  reactors = init_reactors();
        assert_eq!(reactors.len(), 2);

        // Run the front-end I/O.
        // Only poll once to allow the I/O to be issued but not to complete.
        // This ensures the lock is held for an extended period.
        reactors[0].send_future(fut1);
        reactors[0].poll_once();

        // Attempt to acquire the lock
        reactors[1].send_future(fut2);

        // Lock request should time out - the front-end I/O is still holding the lock
        select! {
            recv(r) -> _ => panic!("Shouldn't be able to acquire lock"),
            recv(after(Duration::from_secs(5))) -> _ => println!("Success"),
        }

        // Now the lock request has timed out, poll to allow the front-end I/O
        // to complete and release the lock
        reactor_poll!(100);

        // Lock request should succeed - the front-end I/O has completed
        select! {
            recv(r) -> _ => println!("Success"),
            recv(after(Duration::from_secs(5))) -> _ => panic!("Shouldn't be able to acquire lock"),
        }

        test_fini();
    }).join();
}

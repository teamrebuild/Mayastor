#[macro_use]
extern crate log;

pub mod common;

use crossbeam::channel::{after, select, unbounded};
use futures::channel::oneshot;
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

// Sleep for the specified duration then signal the reactor that the sleep has
// completed.
fn sleep_and_alert(d: Duration, r: &Reactor, s: oneshot::Sender<()>) {
    sleep(d);
    r.send_future(async move {
        let _ = s.send(());
    })
}

#[test]
// Test acquiring and releasing a lock.
fn lock_unlock() {
    let (s, r) = unbounded::<()>();

    std::thread::spawn(move || {
        test_ini();
        Reactor::block_on(async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 5).await;
            arbiter.unlock(&mut ctx).await;
            let _ = s.send(());
        });
        test_fini();
    });

    select! {
        recv(r) -> _ => trace!("Success"),
        recv(after(Duration::from_secs(1))) -> _ => panic!("Timed out"),
    }
}

#[test]
// Test multiple lock requests for an overlapping LBA.
// Unlocking is not performed, therefore only the first lock request should
// succeed.
fn multiple_locks() {
    let (s, r) = unbounded::<()>();

    std::thread::spawn(move || {
        test_ini();
        Reactor::block_on(async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            arbiter.lock(1, 10).await;
            arbiter.lock(1, 5).await;

            let _ = s.send(());
        });
        test_fini();
    });

    // Expect a timeout - the first lock request is never unlocked, so the
    // second lock request should never succeed.
    select! {
        recv(r) -> _ => panic!("Shouldn't succeed on double lock"),
        recv(after(Duration::from_secs(1))) -> _ => println!("Success"),
    }
}

#[async_std::test]
// Test multiple lock requests for an overlapping LBA range with unlocking.
// The first lock is acquired and only unlocked after the timer times out.
// Whilst the first lock is held, the second lock request should be blocked.
// The second lock request should only succeed after the first lock is unlocked.
async fn multiple_locks_with_unlocks() {
    let (s, r) = unbounded::<()>();
    let (timer_sender, timer_receiver) = oneshot::channel::<()>();

    std::thread::spawn(move || {
        test_ini();

        // Acquire lock and hold it for an extended duration.
        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);

            trace!("fut1: REQUEST LOCK");
            let mut ctx = arbiter.lock(1, 10).await;
            trace!("fut1: ACQUIRED LOCK");

            // Wait for timer to expire
            let _ = timer_receiver.await;

            arbiter.unlock(&mut ctx).await;
            trace!("fut1: RELEASED LOCK");
        };

        // Attempt to acquire the lock whilst it is already being held.
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

        // Configure the reactor
        Reactors::init();
        let reactor = Reactors::current();
        reactor.running();
        reactor.send_future(fut1);
        reactor.send_future(fut2);

        // Set up timer thread before polling the reactor and running the above
        // futures.
        std::thread::spawn(move || {
            sleep_and_alert(Duration::from_secs(1), &reactor, timer_sender);
        });

        reactor.poll_reactor();

        test_fini();
    });

    select! {
        recv(r) -> _ => trace!("Success"),
        recv(after(Duration::from_secs(5))) -> _ => panic!("Timed out"),
    }
}

#[test]
// Test issuing a front-end I/O to a LBA range that overlaps a locked range.
// The front-end I/O should never complete because the lock is never unlocked.
fn fe_io_lock_held() {
    let (s, r) = unbounded::<()>();
    let (s1, r1) = oneshot::channel::<()>();

    std::thread::spawn(move || {
        test_ini();

        // Future 1, acquire lock
        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            arbiter.lock(1, 5).await;
            trace!("fut1: LOCKED");
            let _ = s1.send(());
        };

        // Future 2, issue I/O to nexus
        let fut2 = async move {
            let nexus_desc = Bdev::open_by_name(&NEXUS_NAME, true).unwrap();
            let h = nexus_desc.into_handle().unwrap();

            let blk = 2;
            let blk_size = 512;
            let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

            // Wait for fut1 to obtain lock
            let _ = r1.await;

            trace!("fut2: write to nexus");
            match h.write_at((blk * blk_size) as u64, &buf).await {
                Ok(_) => trace!("Successfully wrote to nexus"),
                Err(e) => trace!("Failed to write to nexus: {}", e),
            }

            let _ = s.send(());
        };

        // Configure the reactor
        Reactors::init();
        let reactor = Reactors::current();
        reactor.running();
        reactor.send_future(fut1);
        reactor.send_future(fut2);
        reactor.poll_reactor();

        test_fini();
    });

    // Expect a timeout - the nexus shouldn't be able to be written to while a
    // lock is held on an overlapping LBA range
    select! {
        recv(r) -> _ => panic!("Shouldn't be able to write to nexus"),
        recv(after(Duration::from_secs(5))) -> _ => println!("Success"),
    }
}

#[test]
// Test issuing a front-end I/O to a LBA range that overlaps a locked range but
// which is subsequently unlocked.
// The front-end I/O should eventually complete after the lock is unlocked.
fn fe_io_lock_unlock() {
    let (s, r) = unbounded::<()>();
    let (s1, r1) = oneshot::channel::<()>();
    let (timer_sender, timer_receiver) = oneshot::channel::<()>();

    std::thread::spawn(move || {
        test_ini();

        // Future 1, acquire lock and unlock after delay
        let fut1 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 5).await;
            info!("fut1: LOCKED");
            let _ = s1.send(());

            // Wait for timer to expire
            let _ = timer_receiver.await;

            arbiter.unlock(&mut ctx).await;
            info!("fut1: UNLOCKED");
        };

        // Future 2, issue I/O to nexus
        let fut2 = async move {
            let nexus_desc = Bdev::open_by_name(&NEXUS_NAME, true).unwrap();
            let h = nexus_desc.into_handle().unwrap();

            let blk = 2;
            let blk_size = 512;
            let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

            // Wait for f1 to obtain lock
            let _ = r1.await;

            info!("fut2: write to nexus");
            match h.write_at((blk * blk_size) as u64, &buf).await {
                Ok(_) => info!("Successfully wrote to nexus"),
                Err(e) => info!("Failed to write to nexus: {}", e),
            }

            let _ = s.send(());
        };

        // Configure the reactor
        Reactors::init();
        let reactor = Reactors::current();
        reactor.running();
        reactor.send_future(fut1);
        reactor.send_future(fut2);

        // Set up timer thread before polling the reactor and running the above
        // futures.
        std::thread::spawn(move || {
            sleep_and_alert(Duration::from_secs(1), &reactor, timer_sender);
        });

        reactor.poll_reactor();

        test_fini();
    });

    select! {
        recv(r) -> _ => println!("Success"),
        recv(after(Duration::from_secs(5))) -> _ => panic!("Expected front end write to succeed"),
    }
}

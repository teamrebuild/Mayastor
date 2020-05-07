pub mod common;

use mayastor::{
    bdev::nexus_create,
    core::{MayastorCliArgs, MayastorEnvironment, Reactor},
    replicas::io_arbiter::IoArbiter,
};

use crossbeam::channel::{after, select, unbounded};
use mayastor::bdev::nexus_lookup;
use std::{thread::sleep, time::Duration};

use mayastor::core::Reactors;

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

#[test]
fn test_lock_unlock() {
    let (s, r) = unbounded::<()>();

    std::thread::spawn(move || {
        test_ini();
        Reactor::block_on(async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 1).await;
            arbiter.unlock(&mut ctx).await;
            let _ = s.send(());
        });
        test_fini();
    });

    select! {
        recv(r) -> _ => println!("Success"),
        recv(after(Duration::from_secs(1))) -> _ => panic!("Timed out"),
    }
}

#[test]
fn test_double_lock() {
    let (s, r) = unbounded::<()>();

    std::thread::spawn(move || {
        test_ini();
        Reactor::block_on(async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            arbiter.lock(1, 1).await;
            arbiter.lock(1, 1).await;
            let _ = s.send(());
        });
        test_fini();
    });

    select! {
        recv(r) -> _ => panic!("Shouldn't succeed on double lock"),
        recv(after(Duration::from_secs(1))) -> _ => println!("Success"),
    }
}

#[async_std::test]
async fn test_double_lock_unlock() {
    let (s, r) = unbounded::<()>();

    std::thread::spawn(move || {
        test_ini();

        let f1 = async {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            let mut ctx = arbiter.lock(1, 1).await;
            println!("**** F1 HOLD LOCK ****");
            sleep(Duration::from_secs(2));
            println!("Finished sleep, unlock");
            println!("**** F1 RELEASE LOCK ****");
            arbiter.unlock(&mut ctx).await;
        };

        let f2 = async move {
            let mut arbiter = IoArbiter::new(NEXUS_NAME);
            println!("**** F2 WAIT FOR LOCK ****");
            let mut ctx = arbiter.lock(1, 1).await;
            println!("Got lock");
            arbiter.unlock(&mut ctx).await;
            let _ = s.send(());
        };

        Reactors::init();
        let reactor = Reactors::current();
        reactor.running();
        reactor.send_future(f1);
        reactor.send_future(f2);
        reactor.poll_reactor();

        test_fini();
    });

    select! {
        recv(r) -> _ => println!("Success"),
        recv(after(Duration::from_secs(5))) -> _ => panic!("Timed out"),
    }
}

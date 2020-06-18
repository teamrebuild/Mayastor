use crossbeam::atomic::AtomicCell;
use once_cell::sync::Lazy;

use mayastor::core::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    Reactor,
    Reactors,
};

pub mod common;

static COUNT: Lazy<AtomicCell<u32>> = Lazy::new(|| AtomicCell::new(0));

#[test]
fn reactor_block_on() {
    common::mayastor_test_init();
    let ms = MayastorEnvironment::new(MayastorCliArgs::default());
    ms.start(|| {
        Reactor::block_on(async move {
            assert_eq!(COUNT.load(), 0);
            COUNT.store(1);
            Reactors::master()
                .send_future(async { assert_eq!(COUNT.load(), 2) });
            Reactor::block_on(async {
                assert_eq!(COUNT.load(), 1);
                COUNT.store(2);
            });
        });
        mayastor_env_stop(0);
    })
    .unwrap();

    assert_eq!(COUNT.load(), 2);
}

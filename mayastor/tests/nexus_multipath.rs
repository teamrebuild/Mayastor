//! Multipath NVMf tests
//! Create the same nexus on both nodes with a replica on 1 node their child.
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::MayastorCliArgs,
};
use rpc::mayastor::{
    CreateNexusRequest,
    CreatePoolRequest,
    CreateReplicaRequest,
    PublishNexusRequest,
    ShareProtocolNexus,
    ShareReplicaRequest,
};
use std::process::Command;

pub mod common;
use common::{compose::Builder, MayastorTest};

static POOL_NAME: &str = "tpool";
static UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";
static HOSTNQN: &str = "nqn.2019-05.io.openebs";

#[tokio::test]
async fn nexus_multipath() {
    // create a new composeTest
    let test = Builder::new()
        .name("nexus_shared_replica_test")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let mut hdls = test.grpc_handles().await.unwrap();

    // create a pool on remote node
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();

    // create replica, not shared
    hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 0,
        })
        .await
        .unwrap();

    // create nexus on remote node with local replica as child
    hdls[0]
        .mayastor
        .create_nexus(CreateNexusRequest {
            uuid: UUID.to_string(),
            size: 32 * 1024 * 1024,
            children: [format!("loopback:///{}", UUID)].to_vec(),
        })
        .await
        .unwrap();

    // share replica
    hdls[0]
        .mayastor
        .share_replica(ShareReplicaRequest {
            uuid: UUID.to_string(),
            share: 1,
        })
        .await
        .unwrap();

    let mayastor = MayastorTest::new(MayastorCliArgs::default());
    let ip0 = hdls[0].endpoint.ip();
    let nexus_name = format!("nexus-{}", UUID);
    mayastor
        .spawn(async move {
            // create nexus on local node with remote replica as child
            nexus_create(
                &nexus_name,
                32 * 1024 * 1024,
                Some(UUID),
                &[format!("nvmf://{}:8420/{}:{}", ip0, HOSTNQN, UUID)],
            )
            .await
            .unwrap();
            // publish nexus on local node over nvmf
            nexus_lookup(&nexus_name)
                .unwrap()
                .share(ShareProtocolNexus::NexusNvmf, None)
                .await
                .unwrap();
        })
        .await;

    // publish nexus on other node
    hdls[0]
        .mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: UUID.to_string(),
            key: "".to_string(),
            share: ShareProtocolNexus::NexusNvmf as i32,
        })
        .await
        .unwrap();

    let nqn = format!("{}:nexus-{}", HOSTNQN, UUID);
    let status = Command::new("nvme")
        .args(&["connect"])
        .args(&["-t", "tcp"])
        .args(&["-a", "127.0.0.1"])
        .args(&["-s", "8420"])
        .args(&["-n", &nqn])
        .status()
        .unwrap();
    assert!(
        status.success(),
        "failed to connect to local nexus, {}",
        status
    );

    // The first attempt often fails with "Duplicate cntlid x with y" error from
    // kernel
    for i in 0 .. 2 {
        let status_c0 = Command::new("nvme")
            .args(&["connect"])
            .args(&["-t", "tcp"])
            .args(&["-a", &ip0.to_string()])
            .args(&["-s", "8420"])
            .args(&["-n", &nqn])
            .status()
            .unwrap();
        if i == 0 && status_c0.success() {
            break;
        }
        assert!(
            status_c0.success() || i != 1,
            "failed to connect to remote nexus, {}",
            status_c0
        );
    }

    // NQN:<nqn> disconnected 2 controller(s)
    let output_dis = Command::new("nvme")
        .args(&["disconnect"])
        .args(&["-n", &nqn])
        .output()
        .unwrap();
    assert!(
        output_dis.status.success(),
        "failed to disconnect from nexuses, {}",
        output_dis.status
    );
    let s = String::from_utf8(output_dis.stdout).unwrap();
    let v: Vec<&str> = s.split(' ').collect();
    tracing::info!("nvme disconnected: {:?}", v);
    assert!(v.len() == 4);
    assert!(v[1] == "disconnected");
    assert!(
        v[0] == format!("NQN:{}", &nqn),
        "mismatched NQN disconnected"
    );
    assert!(v[2] == "2", "mismatched number of controllers disconnected");
}

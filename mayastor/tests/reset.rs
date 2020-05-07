pub mod common;
use common::ms_exec::MayastorProcess;
use mayastor::{
    bdev::nexus_create,
    core::{
        mayastor_env_stop,
        BdevHandle,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    subsys,
    subsys::Config,
};
use std::{thread, time};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static NXNAME: &str = "reset_test";

fn generate_config() {
    let uri1 = BDEVNAME1.into();
    let uri2 = BDEVNAME2.into();
    let mut config = Config::default();

    let child1_bdev = subsys::BaseBdev {
        uri: uri1,
        uuid: Some("00000000-76b6-4fcf-864d-1027d4038756".into()),
    };

    let child2_bdev = subsys::BaseBdev {
        uri: uri2,
        uuid: Some("11111111-76b6-4fcf-864d-1027d4038756".into()),
    };

    config.base_bdevs = Some(vec![child1_bdev]);
    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.replica_port = 8430;
    config.write("/tmp/child1.yaml").unwrap();

    config.base_bdevs = Some(vec![child2_bdev]);
    config.nexus_opts.replica_port = 8431;
    config.write("/tmp/child2.yaml").unwrap();
}

#[test]
fn nexus_reset_mirror() {
    generate_config();

    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        "/tmp/child1.yaml".to_string(),
        "-p".into(),
        "10126".into(),
    ];

    let mut ms1 = MayastorProcess::new(Box::from(args)).unwrap();

    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        "/tmp/child2.yaml".to_string(),
        "-p".into(),
        "10127".into(),
    ];

    let _ms2 = MayastorProcess::new(Box::from(args)).unwrap();

    test_init!();

    Reactor::block_on(async {
        create_nexus().await;
        reset().await;
        write_some().await;
        read_some().await;
    });
    ms1.sig_term();
    thread::sleep(time::Duration::from_secs(1));
    Reactor::block_on(async {
        read_some().await;
        write_some().await;
    });
    mayastor_env_stop(0);
}

async fn create_nexus() {
    let ch = vec![
        "nvmf://127.0.0.1:8431/nqn.2019-05.io.openebs:11111111-76b6-4fcf-864d-1027d4038756".to_string(),
        "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:00000000-76b6-4fcf-864d-1027d4038756".into()
    ];

    nexus_create(NXNAME, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn reset() {
    let bdev = BdevHandle::open(NXNAME, true, false).unwrap();
    bdev.reset().await.unwrap();
}

async fn write_some() {
    let bdev = BdevHandle::open(NXNAME, true, false).unwrap();
    let mut buf = bdev.dma_malloc(512).expect("failed to allocate buffer");
    buf.fill(0xff);

    let s = buf.as_slice();
    assert_eq!(s[0], 0xff);

    bdev.write_at(0, &buf).await.unwrap();
}

async fn read_some() {
    let bdev = BdevHandle::open(NXNAME, true, false).unwrap();
    let mut buf = bdev.dma_malloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[513] = 0xff;
    assert_eq!(slice[513], 0xff);

    bdev.read_at(0, &mut buf).await.unwrap();

    let slice = buf.as_slice();

    assert_eq!(slice[0], 0xff);
    assert_eq!(slice[513], 0);
}

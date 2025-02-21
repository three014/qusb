use core::str;
use std::{
    io::{stdout, BufWriter},
    sync::Mutex,
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use tracing_subscriber::{fmt::MakeWriter, EnvFilter};

mod common;

// #[tokio::test]
// async fn can_connect_client_to_server() {
//     let addr = common::addr(7000);
//     let (client, server) = common::localhost(addr);
//     let handle = server.serve();
//     let _session = client.connect(addr, "localhost").await.unwrap();
//     drop(_session);
//     handle.shutdown().await.unwrap().unwrap();
// }

// #[tokio::test]
// #[tracing_test::traced_test]
// async fn list_devices_works() {
//     let addr = common::addr(7001);
//     let (client, server) = common::localhost(addr);
//     let handle = server.serve();

//     {
//         let session = client.connect(addr, "localhost").await.unwrap();
//         tracing::info!("Connected to {}", session.remote_address());

//         let result = session.req_list_devices().await;
//         let devices = {
//             tokio::time::sleep(Duration::from_secs(5)).await;
//             result.unwrap()
//         };
//         for dev in devices.iter() {
//             println!("=========================================");
//             println!("{:?}", dev.path());
//             println!("{:?}", dev.header);
//             println!(
//                 "{:?}",
//                 &dev.interfaces[..dev.header.b_num_interfaces as usize]
//             );
//         }
//     }

//     handle.shutdown().await.unwrap().unwrap();
// }

// #[test]
fn borrow_self_dev() {
    let log_path = "borrow_self_dev.log";
    let log_file = std::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();
    _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::builder().parse("none,qusb=trace").unwrap())
        .with_writer(Mutex::new(BufWriter::with_capacity(1024, log_file)))
        .try_init();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_keep_alive(Duration::from_secs(60))
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, borrow_self_dev_inner());
}

async fn borrow_self_dev_inner() {
    let addr = common::addr(7002);
    let (client, server) = common::localhost(addr);
    let handle = server.serve();
    let ctrl_c = tokio::signal::ctrl_c();

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let usb = session
            .req_borrow(proto::msg::UsbDeviceId {
                bus_number: 1,
                device_addr: 13,
            })
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        let mut handle = tokio::task::spawn_local(usb.borrow(cancel.clone()));
        tokio::select! {
            result = &mut handle => {
                result.unwrap().unwrap();
            }
            result = ctrl_c => {
                result.unwrap();
                handle.await.unwrap().unwrap();
            }
        }
    }

    handle.shutdown().await.unwrap().unwrap();
}

// #[tokio::test]
// async fn server_works() {
//     let log_path = "server.log";
//     let log_file = std::fs::File::options()
//         .create(true)
//         .read(true)
//         .write(true)
//         .truncate(true)
//         .open(log_path)
//         .unwrap();
//     _ = tracing_subscriber::fmt()
//         .with_env_filter(EnvFilter::builder().parse("none,qusb=trace").unwrap())
//         .with_line_number(true)
//         .with_writer(Mutex::new(BufWriter::new(log_file)))
//         .try_init();

//     let server = common::dummy_server("[::]:7400".parse().unwrap());
//     let handle = server.serve();

//     tokio::time::sleep(Duration::from_secs(120)).await;
//     handle.shutdown().await.unwrap().unwrap();
// }

#[test]
fn client_borrows_usb() {
    let log_path = "client_borrows_usb.log";
    let log_file = std::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();
    _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::builder().parse("none,qusb=trace").unwrap())
        .with_line_number(true)
        .with_writer(Mutex::new(BufWriter::with_capacity(128, log_file)))
        .try_init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_keep_alive(Duration::from_secs(60))
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let client = common::dummy_trusting_client("[::]:7400".parse().unwrap());
        let session = client
            .connect("10.4.31.230:7400".parse().unwrap(), "pan1.test.bed")
            .await
            .unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let usb = session
            .req_borrow(proto::msg::UsbDeviceId {
                bus_number: 9,
                device_addr: 2,
            })
            .await
            .unwrap();
        let ctrl_c = tokio::signal::ctrl_c();
        let cancel = CancellationToken::new();
        let mut handle = tokio::task::spawn(usb.borrow(cancel.clone()));
        tokio::select! {
            result = &mut handle => {
                result.unwrap().unwrap();
            }
            result = ctrl_c => {
                result.unwrap();
                cancel.cancel();
                handle.await.unwrap().unwrap();
            }
        }
    });
}

use core::str;
use std::time::Duration;

use tracing_subscriber::EnvFilter;

mod common;

#[tokio::test]
async fn can_connect_client_to_server() {
    let addr = common::addr(7000);
    let (client, server) = common::localhost(addr);
    let handle = server.serve();
    let _session = client.connect(addr, "localhost").await.unwrap();
    drop(_session);
    handle.shutdown().await.unwrap().unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn list_devices_works() {
    let addr = common::addr(7001);
    let (client, server) = common::localhost(addr);
    let handle = server.serve();

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let result = session.req_list_devices().await;
        let devices = {
            tokio::time::sleep(Duration::from_secs(5)).await;
            result.unwrap()
        };
        for dev in devices.iter() {
            println!("=========================================");
            println!("{:?}", dev.path());
            println!("{:?}", dev.header);
            println!(
                "{:?}",
                &dev.interfaces[..dev.header.b_num_interfaces as usize]
            );
        }
    }

    handle.shutdown().await.unwrap().unwrap();
}

#[tokio::test]
async fn borrow_self_dev() {
    let log_path = "log.txt";
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
        .with_writer(log_file)
        .try_init();

    let addr = common::addr(7002);
    let (client, server) = common::localhost(addr);
    let handle = server.serve();

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let usb = session
            .borrow_device(proto::msg::UsbDeviceId {
                bus_number: 3,
                device_addr: 62,
            })
            .await
            .unwrap();
        usb.borrow().await.unwrap();
    }

    handle.shutdown().await.unwrap().unwrap();
}

#[tokio::test]
async fn server_with_keyboard() {
    let log_path = "log.txt";
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
        .with_writer(log_file)
        .try_init();

    let server = common::dummy_server("0.0.0.0:7400".parse().unwrap());
    let handle = server.serve();

    tokio::time::sleep(Duration::from_secs(120)).await;
    handle.shutdown().await.unwrap().unwrap();
}

#[tokio::test]
async fn client_wants_keyboard() {
    let log_path = "log.txt";
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
        .with_writer(log_file)
        .try_init();

    let client = common::dummy_trusting_client("0.0.0.0:7400".parse().unwrap());
    let session = client
        .connect("10.4.31.230:7400".parse().unwrap(), "pan1.test.bed")
        .await
        .unwrap();
    tracing::info!("Connected to {}", session.remote_address());

    let devices = session.req_list_devices().await.unwrap();

    // Drop Keyboard: idVendor=0c45, idProduct=7016
    let keyboard = devices
        .iter()
        .find(|&dev| 0x0c45 == dev.header.id_vendor && 0x7016 == dev.header.id_product)
        .unwrap();

    let usb = session
        .borrow_device(proto::msg::UsbDeviceId {
            bus_number: keyboard.header.busnum,
            device_addr: keyboard.header.devnum,
        })
        .await
        .unwrap();
    usb.borrow().await.unwrap();
}

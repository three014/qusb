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
async fn send_usb_data() {
    let log_path = "log.txt";
    let log_file = std::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();
    _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .parse("none,qusb=trace")
                .unwrap(),
        )
        .with_line_number(true)
        .with_writer(log_file)
        .try_init();

    let addr = common::addr(7002);
    let (client, server) = common::localhost(addr);
    let handle = server.serve();

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let devices = session.req_list_devices().await.unwrap();

        // Sandisk Flash Drive: 0781:5575
        // Drop Keyboard: 0c45:7016
        // Fingerprint Sensor: 27c6:63ac
        // let keyboard = devices
        //     .iter()
        //     .find(|&dev| 0x27c6 == dev.header.id_vendor && 0x63ac == dev.header.id_product)
        //     .unwrap();

        let usb = session
            .borrow_device(proto::msg::UsbDeviceId {
                bus_number: 0x003,
                device_addr: 0x002,
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
        .with_env_filter(
            EnvFilter::builder()
                .parse("none,qusb=trace")
                .unwrap(),
        )
        .with_line_number(true)
        .with_writer(log_file)
        .try_init();

    let addr = common::addr(7003);
    let server = common::dummy_server(addr);
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
        .with_env_filter(
            EnvFilter::builder()
                .parse("none,qusb=trace")
                .unwrap(),
        )
        .with_line_number(true)
        .with_writer(log_file)
        .try_init();

    let addr = common::addr(7003);
    let client = common::dummy_trusting_client(addr);
    let session = client.connect("10.4.31.230:7003".parse().unwrap(), "pan1.garden.lan").await.unwrap();
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

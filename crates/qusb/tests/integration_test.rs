use core::str;
use std::time::Duration;

mod common;

#[tokio::test]
async fn can_connect_client_to_server() {
    let addr = common::addr(7000);
    let (client, server) = common::setup(addr);
    let handle = server.serve();
    let _session = client.connect(addr, "localhost").await.unwrap();
    drop(_session);
    handle.shutdown().await.unwrap().unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn list_devices_works() {
    let addr = common::addr(7001);
    let (client, server) = common::setup(addr);
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
    let _ = tracing_subscriber::fmt::try_init();

    let addr = common::addr(7002);
    let (client, server) = common::setup(addr);
    let handle = server.serve();

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let devices = session.req_list_devices().await.unwrap();

        let belkin = devices
            .iter()
            .find(|&dev| 0x050d == dev.header.id_vendor && 0x0200 == dev.header.id_product)
            .unwrap();

        let usb = session
            .borrow_device(proto::msg::UsbDeviceId {
                bus_number: belkin.header.busnum,
                device_addr: belkin.header.devnum,
            })
            .await
            .unwrap();
        usb.borrow().await;
    }
}

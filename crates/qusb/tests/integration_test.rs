use core::str;

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

        let _devices = session.req_list_devices().await.unwrap();
        // println!("{devices:?}");
    }

    handle.shutdown().await.unwrap().unwrap();
}

mod common;

#[tokio::test]
async fn can_connect_client_to_server() {
    let addr = common::addr(7000);
    let (client, server) = common::setup(addr);
    let handle = server.serve(|_session, _canceler| async move { Ok(()) });
    let _session = client.connect(addr, "localhost").await.unwrap();
    drop(_session);
    handle.shutdown().await.unwrap().unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn list_devices_works() {
    let addr = common::addr(7001);
    let (client, server) = common::setup(addr);
    let handle = server.serve(|session, _canceler| async move {
        let stream = session
            .accept_stream()
            .await
            .unwrap()
            .recv_req()
            .await
            .unwrap();
        tracing::trace!("Accepted new stream");

        let req = stream.req();
        tracing::trace!("Received request from client: {req:?}");
        match req {
            proto::Request::ListUsbDevices => {
                qusb::handle_list_devices(stream).await?;
            }
            proto::Request::Borrow(_) => panic!("Not what this test is for"),
        }

        tracing::trace!("Finished serving req");
        Ok(())
    });

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        tracing::info!("Connected to {}", session.remote_address());

        let _devs = session.req_list_devices().await.unwrap();
    }

    handle.shutdown().await.unwrap().unwrap();
}

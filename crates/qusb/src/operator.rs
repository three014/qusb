use std::{
    io,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use bytes::{Buf, BytesMut};
use nohash_hasher::{IntMap, IntSet};
use proto::{
    data::{Data, ReadError, Ring},
    msg::{self, Header, QusbFrame, UrbFrame, UrbHeader},
};
use rand::Rng;
use rusb::UsbContext;
use rusb_async::{InnerTransfer, IsoPacket, TransferFlags, TransferStatus};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn, Instrument};
use vhci::{
    ioctl::{self, UrbType},
    usbfs::{Dir, Request},
    DataRate, PortChange, PortFlag, PortStatus,
};
use zerocopy::{FromBytes, IntoBytes};

use crate::{
    stub::{self, RegisterPort},
    utils::{self, align_to_usize, SimpleMap},
    Error, Result, RusbError, UrbWithIsoData, UrbWithIsoGiveback,
};

/// Rust-representation of a peer request
pub enum ClientReq {
    ListDevices,
    BorrowDevice(msg::UsbDeviceId),
}

const HEADER_SIZE: usize = size_of::<Header>();

enum Recv {
    Urb((Header, Data<UrbFrame>)),
    PortReset(Header),
    Unlink(Header),
}

async fn recv_frame<R: AsyncRead + Unpin>(mut rx: R, buf: &mut Ring) -> io::Result<Option<Recv>> {
    let mut min_len = HEADER_SIZE;
    let frame: Data<QusbFrame> = loop {
        if buf.fill_until(&mut rx, min_len).await?.is_none() {
            return Ok(None);
        }

        match buf.claim_dst() {
            Ok(frame) => break frame,
            Err(ReadError::CorruptedData) => todo!(),
            Err(ReadError::BufferShort {
                num_bytes_needed, ..
            }) => {
                min_len = buf.len() + num_bytes_needed;
            }
        }
    };

    let frame_ref = frame.get();
    match frame_ref.header.command {
        msg::Command::CmdUnlink => Ok(Some(Recv::Unlink(frame_ref.header.clone()))),
        msg::Command::CmdPort | msg::Command::RetPort => {
            Ok(Some(Recv::PortReset(frame_ref.header.clone())))
        }
        msg::Command::RetSubmit | msg::Command::CmdSubmit => {
            // In this case this will probably
            // be faster since we already parsed
            // the header
            let header = frame_ref.header.clone();
            let (_, urb) = frame.split::<UrbFrame>();
            Ok(Some(Recv::Urb((header, urb))))
        }
        _ => unreachable!("client smh smh"),
    }
}

/// A struct containing the logic for
/// borrowing a USB device from a lender.
pub struct ClientBorrowDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    vhci: stub::Controller,
    id: msg::UsbDeviceId,
}

impl<W, R> ClientBorrowDevice<W, R> {
    pub fn new(tx: W, rx: R, buf_rx: Ring, vhci: stub::Controller, id: msg::UsbDeviceId) -> Self {
        Self {
            tx,
            rx,
            buf_rx,
            vhci,
            id,
        }
    }
}

impl<W, R> ClientBorrowDevice<W, R>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
    R: AsyncRead + Unpin,
{
    /// Runs the logic for sending URBs from a VHCI
    /// to a remote host that has access to the real USB
    /// device.
    ///
    /// Currently, the only way to stop this function is to
    /// disconnect a USB device from the VHCI or to just not
    /// poll the future, though the function does not have
    /// graceful shutdown at the moment.
    ///
    /// # Cancel safety
    ///
    /// Due to the reasons above, this function is not cancel
    /// safe.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn borrow(self) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            mut vhci,
            id: _dev_id,
        } = self;
        const BUF_LEN: usize = 16 << 10;
        let mut scratch_buf = [0u8; BUF_LEN];
        buf_rx.reserve(8192);

        let (port, mut work_rx) = vhci
            .register(RegisterPort::Any, DataRate::High)
            .await
            .unwrap();

        enum Event {
            Work(Option<ioctl::Work>),
            Frame(io::Result<Option<Recv>>),
        }

        let seqnum = AtomicU32::new(0);
        let mut addr: u8 = 0xff;
        let mut prev = ioctl::IocPortStat::default();
        let mut handles =
            SimpleMap::<u32, ioctl::UrbHandle>::with_capacity_and_hasher(32, Default::default());
        let mut seqnums =
            SimpleMap::<ioctl::UrbHandle, u32>::with_capacity_and_hasher(32, Default::default());
        let mut handle_rng = rand::thread_rng();

        info!("starting event loop");

        let result: Result<()> = loop {
            debug!("==============================================");
            let event = tokio::select! {
                maybe_work = work_rx.recv() => {
                    Event::Work(maybe_work)
                }
                maybe_frame = recv_frame(&mut rx, &mut buf_rx) => {
                    Event::Frame(maybe_frame)
                }
            };

            match event {
                Event::Work(Some(ioctl::Work::PortStat(next))) => {
                    debug!("got port stat for {:?}", port);
                    // debug!("status: {:?}", next.status());
                    // debug!("change: {:?}", next.change());
                    // debug!("index: {:?}", next.index());
                    // debug!("flags: {:?}", next.flags());
                    let status = next.status();
                    let change = next.change();
                    let flags = next.flags();
                    if change.contains(PortChange::CONNECTION) {
                        debug!("CONNECTION state changed -> invalidating address");
                        addr = 0xff;
                    }
                    if change.contains(PortChange::RESET)
                        && (!status).contains(PortStatus::RESET)
                        && status.contains(PortStatus::ENABLE)
                    {
                        debug!("RESET successful -> use default address");
                        addr = 0;
                    }
                    if prev.status().contains(PortStatus::POWER)
                        && (!status).contains(PortStatus::POWER)
                    {
                        debug!("port is powered off");
                    }
                    if (!prev.status()).contains(PortStatus::RESET)
                        && status.contains(PortStatus::RESET | PortStatus::CONNECTION)
                    {
                        let next_seqnum = seqnum.fetch_add(1, Ordering::Relaxed);
                        // We pray that we don't run into another handle
                        let handle = ioctl::UrbHandle(handle_rng.gen());
                        seqnums.insert(handle, next_seqnum);
                        handles.insert(next_seqnum, handle);
                        debug!("({next_seqnum}) port is resetting");
                        let now = Instant::now();

                        let header = Header {
                            total_frame_len: (size_of::<Header>() / 8) as u16,
                            seqnum: next_seqnum,
                            command: msg::Command::CmdPort,
                            status: msg::Status::Success,
                        };

                        let elapsed = now.elapsed();
                        let mut request = header.as_bytes();
                        trace!("took {elapsed:?} to setup port reset frame");
                        // trace!(
                        //     "seqnum {} - request is {} bytes",
                        //     header.seqnum,
                        //     request.len()
                        // );
                        tx.write_all_buf(&mut request).await.unwrap();
                    }
                    if (!prev.flags()).contains(PortFlag::RESUMING)
                        && flags.contains(PortFlag::RESUMING)
                        && status.contains(PortStatus::CONNECTION)
                    {
                        debug!("port is resuming -> completing resume");
                        // todo!("do the actual resume thing");
                    }
                    prev = next;
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle))))
                    if UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
                {
                    addr = urb.setup_packet.value().try_into().unwrap();
                    debug!("set address to {addr:#x}");

                    struct EmptyUrb {
                        handle: ioctl::UrbHandle,
                        ioctl_urb: ioctl::IocUrb,
                    }

                    impl vhci::Urb for EmptyUrb {
                        fn kind(&self) -> ioctl::UrbType {
                            self.ioctl_urb.typ
                        }

                        fn handle(&self) -> ioctl::UrbHandle {
                            self.handle
                        }

                        fn status(&self) -> vhci::Status {
                            vhci::Status::Success
                        }

                        fn dir(&self) -> vhci::usbfs::Dir {
                            if UrbType::Ctrl == self.kind() {
                                self.ioctl_urb.setup_packet.req().dir()
                            } else {
                                self.ioctl_urb.endpoint.direction()
                            }
                        }

                        fn bytes_transferred(&self) -> u16 {
                            self.ioctl_urb.buffer_length as u16
                        }
                    }

                    impl vhci::TransferMut for EmptyUrb {
                        fn transfer_mut(&mut self) -> &mut [u8] {
                            &mut []
                        }
                    }

                    impl vhci::IsoPacketGivebackMut for EmptyUrb {
                        fn iso_packet_giveback_mut(
                            &mut self,
                        ) -> &mut [ioctl::IocIsoPacketGiveback] {
                            &mut []
                        }

                        fn error_count(&self) -> u16 {
                            0
                        }
                    }

                    let urb = EmptyUrb {
                        handle,
                        ioctl_urb: urb,
                    };

                    vhci.giveback_urb(urb).await.unwrap();
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle)))) => {
                    assert_eq!(addr, urb.address.get());
                    debug!("got urb with {addr:#x}");
                    let now = Instant::now();

                    let next_seqnum = seqnum.fetch_add(1, Ordering::Relaxed);
                    assert!(handles.insert(next_seqnum, handle).is_none());
                    assert!(seqnums.insert(handle, next_seqnum).is_none());

                    // Calculating all parts of the transfer frame
                    let actual_transfer_len = urb.buffer_length as usize;
                    let packet_count = urb.packet_count as usize;

                    let padded_transfer_len = align_to_usize(actual_transfer_len);
                    let data_len =
                        padded_transfer_len + packet_count * size_of::<ioctl::IocIsoPacketData>();
                    let total_frame_len = {
                        let static_len = size_of::<Header>() + size_of::<UrbHeader>();
                        if Dir::Out == urb.endpoint.direction()
                            && (actual_transfer_len > 0 || packet_count > 0)
                        {
                            static_len + data_len
                        } else {
                            static_len
                        }
                    };

                    let header = Header {
                        total_frame_len: (total_frame_len / 8) as u16,
                        seqnum: next_seqnum,
                        command: msg::Command::CmdSubmit,
                        status: msg::Status::Success,
                    };
                    let urb_header = msg::UrbHeader {
                        kind: urb.typ,
                        actual_transfer_len: actual_transfer_len as u16,
                        iso_packet_count: packet_count as u16,
                        interval: urb.interval as u16,
                        // Remove URB_NO_TRANSFER_DMA_MAP flag
                        flags: urb.flags & !0x04,
                        endpoint: urb.endpoint,
                        num_errors: 0,
                        status: vhci::Status::Pending,
                        ctrl_packet: urb.setup_packet,
                    };

                    match urb.endpoint.direction() {
                        Dir::Out if actual_transfer_len > 0 || packet_count > 0 => {
                            debug!(
                                "fetching data (transfer_len: {}, iso_packet_len: {})",
                                actual_transfer_len, packet_count
                            );

                            let data = &mut scratch_buf[..data_len];

                            // Grab mutable references for each part of our frame
                            let (transfer, rest) = data.split_at_mut(padded_transfer_len);
                            let iso_data = <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(
                                rest,
                                packet_count,
                            )
                            .unwrap();

                            let borrower_urb = UrbWithIsoData {
                                handle,
                                header: &urb_header,
                                transfer: &mut transfer[..actual_transfer_len],
                                iso_data,
                            };

                            vhci.fetch_data(borrower_urb).unwrap();
                            let mut request = header
                                .as_bytes()
                                .chain(urb_header.as_bytes())
                                .chain(data.as_bytes());

                            let elapsed = now.elapsed();
                            trace!("took {:?} to setup URB frame for sending", elapsed);
                            // trace!(
                            //     "seqnum {} - request is {} bytes",
                            //     header.seqnum,
                            //     request.remaining()
                            // );
                            tx.write_all_buf(&mut request).await.unwrap();
                        }
                        Dir::Out | Dir::In => {
                            let mut request = header.as_bytes().chain(urb_header.as_bytes());
                            let elapsed = now.elapsed();
                            trace!("took {:?} to setup URB frame for sending", elapsed);
                            // trace!(
                            //     "seqnum {} - request is {} bytes",
                            //     header.seqnum,
                            //     request.remaining()
                            // );
                            tx.write_all_buf(&mut request).await.unwrap();
                        }
                    }
                }
                Event::Work(Some(ioctl::Work::CancelUrb(handle))) => {
                    debug!("got cancel urb with {handle:?}");
                    if let Some(&seqnum) = seqnums.get(&handle) {
                        let header = Header {
                            total_frame_len: (size_of::<Header>() / 8) as u16,
                            seqnum,
                            command: msg::Command::CmdUnlink,
                            status: msg::Status::Success,
                        };

                        let mut request = header.as_bytes();
                        // trace!(
                        //     "about to write {} bytes into the buffer with seqnum {seqnum}",
                        //     request.remaining(),
                        // );
                        tx.write_all_buf(&mut request).await.unwrap();
                    } else {
                        trace!("URB with {handle:?} had already been returned");
                    }
                }
                Event::Frame(Ok(Some(Recv::Urb((
                    Header {
                        seqnum,
                        status: msg::Status::Success,
                        ..
                    },
                    mut urb,
                ))))) => {
                    let now = Instant::now();
                    let handle = handles.remove(&seqnum).unwrap();
                    let _ = seqnums.remove(&handle).unwrap();
                    let frame = urb.get_mut();
                    let urb = &mut frame.header;

                    if vhci::Status::Success != urb.status {
                        warn!("transfer {} failed: {:?}", seqnum, urb.status);
                    }
                    // debug!("got response with seqnum {}: {:?}", seqnum, &urb);
                    let transfer_len = urb.actual_transfer_len as usize;

                    // We might not be expecting data if we sent some to the usb device
                    let (transfer, iso_giveback) = match urb.endpoint.direction() {
                        Dir::Out => Default::default(),
                        Dir::In => {
                            let (transfer, rest) = <[u8]>::mut_from_prefix_with_elems(
                                &mut frame.data,
                                align_to_usize(transfer_len),
                            )
                            .unwrap();
                            let iso_packets =
                                <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                                    rest,
                                    urb.iso_packet_count as usize,
                                )
                                .unwrap();
                            (&mut transfer[..transfer_len], iso_packets)
                        }
                    };

                    let lender_urb = UrbWithIsoGiveback {
                        handle,
                        header: urb,
                        transfer,
                        iso_giveback,
                    };

                    let elapsed = now.elapsed();
                    trace!("took {elapsed:?} to unpack URB for giveback");

                    vhci.giveback_urb(lender_urb).await.unwrap();
                    // let size_of_frame = size_of_val(frame);
                    // buf_rx.consume(size_of_frame);
                }
                Event::Frame(Ok(Some(Recv::PortReset(Header {
                    seqnum,
                    status: msg::Status::Success,
                    ..
                })))) => {
                    let handle = handles.remove(&seqnum).unwrap();
                    let _ = seqnums.remove(&handle).unwrap();
                    debug!("port has been reset");
                    vhci.reset_done(port, true).unwrap();
                }
                Event::Frame(Ok(Some(Recv::Urb((Header { status, .. }, _)))))
                | Event::Frame(Ok(Some(Recv::PortReset(Header { status, .. })))) => match status {
                    msg::Status::Failed => todo!(),
                    msg::Status::DevBusy => todo!(),
                    msg::Status::DevErr => todo!(),
                    msg::Status::NoDev => {
                        break Err(io::Error::other("usb device disconnected on lender side").into())
                    }
                    msg::Status::Unexpected => todo!(),
                    msg::Status::VersionMismatch => todo!(),
                    msg::Status::Timeout => todo!(),
                    msg::Status::Proto => todo!(),
                    msg::Status::Success => unreachable!(),
                },
                Event::Frame(Ok(None)) => break Ok(()),
                Event::Frame(Err(err)) => break Err(err.into()),
                Event::Work(None) => todo!("how did we get here? should we shutdown here?"),
                Event::Frame(Ok(Some(Recv::Unlink(_)))) => unreachable!("smh smh server"),
            }
        };

        vhci.disconnect(port).await?;
        result
    }
}

pub enum ServerResp<W, R> {
    ListDevices(SendDevices<W>),
    BorrowDevice(LendDevice<W, R>),
}

fn is_config_active<C: rusb::UsbContext>(handle: &rusb::DeviceHandle<C>, config: u8) -> bool {
    handle
        .device()
        .active_config_descriptor()
        .is_ok_and(|current| config == current.number())
}

#[tracing::instrument(level = "trace", skip_all)]
fn set_config<C: rusb::UsbContext>(
    seqnum: u32,
    config: u8,
    mut claimed_interfaces: MutexGuard<'_, IntSet<u8>>,
    handle: &rusb::DeviceHandle<C>,
) -> vhci::Status {
    match handle.set_active_configuration(config) {
        Ok(_) => {
            for interface in 0..16 {
                if claimed_interfaces.insert(interface) && handle.claim_interface(interface).is_ok()
                {
                    trace!("({seqnum}) claimed interface {interface}");
                }
            }
            if !is_config_active(&handle, config) {
                handle.set_active_configuration(config).unwrap();
            }
            debug!("({seqnum}) set config {config}");
            vhci::Status::Success
        }
        Err(err) => {
            warn! { %err, "({seqnum}) couldn't set configuration" };
            vhci::Status::Stall
        }
    }
}

fn open_device(dev_id: msg::UsbDeviceId) -> rusb::Result<Arc<rusb::DeviceHandle<rusb::Context>>> {
    rusb::Context::new()
        .and_then(|ctx| {
            // ctx.set_log_level(rusb::LogLevel::Debug);
            ctx.devices()
        })
        .and_then(|list| {
            list.iter()
                .find(|dev| {
                    dev_id.bus_number == dev.bus_number() && dev_id.device_addr == dev.address()
                })
                .ok_or(rusb::Error::NoDevice)
        })
        .and_then(|dev| dev.open())
        .and_then(|handle| {
            handle.set_auto_detach_kernel_driver(true)?;
            for interface in 0..=16 {
                if let Ok(true) = handle.kernel_driver_active(interface) {
                    handle.detach_kernel_driver(interface)?;
                }
            }
            handle.unconfigure()?;
            Ok(Arc::new(handle))
        })
}

#[tracing::instrument(level = "trace", skip_all)]
fn port_reset<C: rusb::UsbContext>(
    seqnum: u32,
    dev_id: msg::UsbDeviceId,
    handle: &rusb::DeviceHandle<C>,
) -> msg::Status {
    match handle.reset() {
        Ok(_) => {
            trace!("({seqnum}) port has been reset");
            msg::Status::Success
        }
        Err(err) => {
            error! {
                %err,
                "({seqnum}) error while resetting device {dev_id:?}"
            };
            msg::Status::DevErr
        }
    }
}

// struct TransferData {
//     header: Header,
//     urb: Data<UrbFrame>,
// }

// struct PerformTransfer<C: rusb::UsbContext> {
//     now: Instant,
//     cancel: Option<CancellationToken>,
//     claimed: Arc<Mutex<IntSet<u8>>>,
//     handle: Arc<rusb::DeviceHandle<C>>,
//     data: Option<TransferData>
// }

// impl<C: rusb::UsbContext> std::future::Future for PerformTransfer<C> {
//     type Output = (Header, UrbHeader, BytesMut, BytesMut);

//     fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
//         let TransferData { header, urb } = self.data.take().unwrap();
//         let (urb_header, data) = urb.split::<[u8]>();
//         let mut urb_header = urb_header.read();
//         let mut data = data.into_bytes_mut();
//         let transfer_len = urb_header.transfer_actual_len as usize;
//         let mut transfer_buf = data.split_to(align_to_usize(transfer_len));
//         let ctrl = ioctl::IocSetupPacket::ref_from_bytes(&transfer_buf[..8]).unwrap();

//         let (status, mut transfer, iso_pkts) = match urb_header.kind {
//             _ => todo!(),
//         };
//         todo!()
//     }
// }

pub struct LendDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    id: msg::UsbDeviceId,
}

impl<W, R> LendDevice<W, R> {
    pub fn new(tx: W, rx: R, buf_rx: Ring, id: msg::UsbDeviceId) -> Self {
        Self { tx, rx, buf_rx, id }
    }
}

impl<W, R> LendDevice<W, R>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
    R: AsyncRead + Unpin,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn lend2(self) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            id: dev_id,
        } = self;
        buf_rx.reserve(2048);
        let scratch_buf = Arc::new(Mutex::new(BytesMut::with_capacity(2048)));

        enum Event {
            RecvFrame(Option<Recv>),
            SendFrame((Header, UrbHeader, BytesMut, BytesMut)),
        }

        // TODO: Use global context instead
        let device = match open_device(dev_id) {
            Ok(handle) => {
                let mut response = msg::Status::Success.as_bytes().chain(&[0u8; 7][..]);
                tx.write_all_buf(&mut response).await?;
                handle
            }
            Err(err) => {
                let err = RusbError { kind: err, dev_id };
                let ret_err = Error::from(err);
                let status = msg::Status::from(err);

                let mut response = status.as_bytes().chain(&[0u8; 7][..]);

                tx.write_all_buf(&mut response).await.unwrap();
                tx.close().await.unwrap();
                return Err(ret_err);
            }
        };

        let event_handler = CancellationToken::new();
        let runtime = event_handler.clone();
        let ctx = device.context().clone();
        let event_handler_loop = std::thread::spawn(move || {
            while !runtime.is_cancelled() {
                if let Err(err) = ctx.handle_events(Some(Duration::from_secs(5))) {
                    warn!("error from event handler: {err}");
                }
            }
        });

        let mut cancel_tokens: IntMap<u32, CancellationToken> =
            IntMap::with_capacity_and_hasher(256, Default::default());
        let mut transfers: JoinSet<(Header, UrbHeader, BytesMut, BytesMut)> = JoinSet::new();
        let claimed_interfaces: Arc<Mutex<IntSet<u8>>> = Arc::new(Mutex::new(
            IntSet::with_capacity_and_hasher(16, Default::default()),
        ));

        let result = loop {
            let check_transfer = !transfers.is_empty();
            match tokio::select! {
                biased;
                frame = recv_frame(&mut rx, &mut buf_rx) => {
                    Event::RecvFrame(frame?)
                }
                result = transfers.join_next(), if check_transfer => {
                    Event::SendFrame(result.unwrap().unwrap())
                }
            } {
                Event::RecvFrame(Some(Recv::Urb((header, urb_frame)))) => {
                    let now = Instant::now();
                    let cancel = CancellationToken::new();
                    cancel_tokens.insert(header.seqnum, cancel.clone());

                    let claimed = Arc::clone(&claimed_interfaces);
                    let handle = Arc::clone(&device);
                    let scratch = Arc::clone(&scratch_buf);
                    transfers.spawn(async move {
                        let (urb_header, data) = urb_frame.split::<[u8]>();
                        let mut urb_header = urb_header.read();
                        let ctrl = urb_header.ctrl_packet;
                        let mut data = data.into_bytes_mut();
                        let transfer_len = urb_header.actual_transfer_len as usize;
                        let iso_packet_count = urb_header.iso_packet_count as usize;

                        // If we're expecting data, then setup the buffers from our ring.
                        // Otherwise, reserve space to write data.
                        let (transfer_buf, _iso_raw_buf) = if UrbType::Ctrl == urb_header.kind {
                            let mut transfer = {
                                let mut scratch = scratch.lock().unwrap();
                                let len = scratch.len();
                                let w_length = ctrl.length() as usize;
                                assert_eq!(w_length, transfer_len);
                                let needed = size_of::<ioctl::IocSetupPacket>() + align_to_usize(transfer_len);
                                let additional = needed.saturating_sub(len);
                                scratch.reserve(additional);
                                // dbg!((len, needed, additional));
                                // SAFETY: None of this buf is read, only written to at first.
                                //         Plus we just ensured that we have capacity for this.
                                unsafe {
                                    scratch.set_len(needed);
                                }
                                scratch.split_to(needed)
                            };
                            ctrl.write_to_prefix(&mut transfer).unwrap();
                            if Dir::Out == ctrl.req().dir() {
                                data.as_ref().write_to_suffix(&mut transfer).unwrap();
                            }
                            (transfer.split_to(transfer_len + size_of::<ioctl::IocSetupPacket>()), BytesMut::new())
                        } else {
                            match urb_header.endpoint.direction() {
                                Dir::Out => {
                                    let mut transfer = data.split_to(align_to_usize(transfer_len));
                                    let transfer = transfer.split_to(transfer_len);
                                    (transfer, data)
                                },
                                Dir::In => {
                                    let mut buf = {
                                        let mut scratch = scratch.lock().unwrap();
                                        let len = scratch.len();
                                        let iso_byte_len = iso_packet_count * size_of::<ioctl::IocIsoPacketData>();
                                        let needed = align_to_usize(transfer_len) + iso_byte_len;
                                        if 0 != len {
                                            // I'm not entirely sure how this would be possible
                                            warn!("({}) IN transfer len not 0", header.seqnum);
                                        }
                                        let additional = needed.saturating_sub(len);
                                        scratch.reserve(additional);
                                        // SAFETY: None of this buf is read, only written to at first.
                                        //         Plus we just ensured that we have capacity for this.
                                        unsafe {
                                            scratch.set_len(needed);
                                        }
                                        scratch.split_to(needed)
                                    };
                                    let transfer = buf.split_to(align_to_usize(transfer_len)).split_to(transfer_len);
                                    (transfer, buf)
                                },
                            }
                        };

                        // IDEA: Every branch needs to return:
                        //       - The status given by libusb from the result of the transfer
                        //       - A transfer buffer such that 
                        //         `transfer.len() <= urb_header.actual_transfer_len`
                        //       - An iso packet buffer such that
                        //         `iso_pkts.capacity()` is aligned to 8 bytes
                        let (status, mut transfer, iso_pkts) = match urb_header.kind {
                            UrbType::Ctrl
                                if Request::STANDARD_INTERFACE_SET_INTERFACE == ctrl.req() =>
                            {
                                let now = Instant::now();
                                let setting = ctrl.value() as u8;
                                let interface = ctrl.index() as u8;
                                let status = match handle.set_alternate_setting(interface, setting) {
                                    Ok(_) => vhci::Status::Success,
                                    Err(err) => {
                                        warn! {
                                            %err,
                                            "({}) couldn't set alternate setting {setting} for interface {interface}",
                                            header.seqnum
                                        };
                                        vhci::Status::Stall
                                    },
                                };
                                let elapsed = now.elapsed();
                                trace!("({}) setting alt interface took {elapsed:?}", header.seqnum);

                                (status, BytesMut::new(), BytesMut::new())
                            }
                            UrbType::Ctrl
                                if Request::STANDARD_DEVICE_SET_CONFIGURATION == ctrl.req()
                                    && is_config_active(&handle, ctrl.value() as u8) =>
                            {
                                debug!("({}) config {} is already set", header.seqnum, ctrl.value() as u8);
                                let status = vhci::Status::Success;
                                (status, BytesMut::new(), BytesMut::new())
                            }
                            UrbType::Ctrl
                                if Request::STANDARD_DEVICE_SET_CONFIGURATION == ctrl.req()
                                    && !is_config_active(&handle, ctrl.value() as u8) =>
                            {
                                let handle = Arc::clone(&handle);
                                let desired = ctrl.value() as u8;
                                let set_config = tokio::task::spawn_blocking(move || set_config(header.seqnum, desired, claimed.lock().unwrap(), &handle));

                                let status = set_config.await.unwrap();

                                (status, BytesMut::new(), BytesMut::new())
                            }
                            UrbType::Ctrl => {
                                trace! { %ctrl }
                                let is_get_status = Request::STANDARD_DEVICE_GET_STATUS == ctrl.req();
                                // SAFETY: Transfer buffer is longer than
                                //         required lengths, and setup packet
                                //         contains the right length as well.
                                let mut transfer = unsafe {
                                    InnerTransfer::new(0).into_ctrl(
                                        &handle,
                                        transfer_buf,
                                        Duration::from_millis(900),
                                    )
                                };

                                let elapsed = now.elapsed();
                                trace!("({}) took {elapsed:?} to setup {:?} transfer", header.seqnum, urb_header.kind);
                                let status = match transfer.submit(cancel).await {
                                    Ok(TransferStatus::Completed) => vhci::Status::Success,
                                    Ok(TransferStatus::Error) => vhci::Status::Error,
                                    Ok(TransferStatus::TimedOut) => vhci::Status::TimedOut,
                                    Ok(TransferStatus::Cancelled) => vhci::Status::Canceled,
                                    Ok(TransferStatus::Stall) | Err(rusb::Error::InvalidParam) => {
                                        vhci::Status::Stall
                                    }
                                    Ok(TransferStatus::NoDevice) | Err(rusb::Error::NoDevice) => {
                                        vhci::Status::DeviceDisconnected
                                    }
                                    Ok(TransferStatus::Overflow) => vhci::Status::Babble,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "({}) ctrl transfer failed on {dev_id:?}", header.seqnum };
                                        vhci::Status::Error
                                    }
                                };

                                let mut buf = transfer
                                    .into_buf()
                                    .unwrap_or_default()
                                    .split_off(size_of::<ioctl::IocSetupPacket>());
                                if is_get_status {
                                    // This sets the lowest bit to 1, which
                                    // indicates that our fake USB device is self powered.
                                    buf[0] = 0x01;
                                }
                                (status, buf, BytesMut::new())
                            }
                            UrbType::Int => {
                                let mut transfer = unsafe {
                                    InnerTransfer::new(0).into_int(
                                        &handle,
                                        urb_header.endpoint.0,
                                        transfer_buf,
                                    )
                                };

                                let elapsed = now.elapsed();
                                trace!("({}) took {elapsed:?} to setup {:?} transfer", header.seqnum, urb_header.kind);
                                let status = match transfer.submit(cancel).await {
                                    Ok(TransferStatus::Completed) => vhci::Status::Success,
                                    Ok(TransferStatus::Error) => vhci::Status::Error,
                                    Ok(TransferStatus::TimedOut) => vhci::Status::TimedOut,
                                    Ok(TransferStatus::Cancelled) => vhci::Status::Canceled,
                                    Ok(TransferStatus::Stall) | Err(rusb::Error::InvalidParam) => {
                                        vhci::Status::Stall
                                    }
                                    Ok(TransferStatus::NoDevice) | Err(rusb::Error::NoDevice) => {
                                        vhci::Status::DeviceDisconnected
                                    }
                                    Ok(TransferStatus::Overflow) => vhci::Status::Babble,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "({}) int transfer failed on {dev_id:?}", header.seqnum };
                                        vhci::Status::Error
                                    }
                                };

                                let buf = transfer.into_buf().unwrap_or_default();
                                (status, buf, BytesMut::new())
                            }
                            UrbType::Bulk => {
                                // if let Ok((cbw, _)) = CommandBlockWrapper::read_from_prefix(&buf) {
                                //     trace!("{cbw:?}");
                                // }

                                trace!("bulk transfer length: {}", transfer_buf.capacity());
                                trace!("bulk endpoint: {:?}", urb_header.endpoint);

                                let mut transfer = unsafe {
                                    InnerTransfer::new(0).into_bulk(
                                        &handle,
                                        urb_header.endpoint.0,
                                        TransferFlags::NONE,
                                        transfer_buf,
                                    )
                                };

                                let elapsed = now.elapsed();
                                trace!("({}) took {elapsed:?} to setup {:?} transfer", header.seqnum, urb_header.kind);
                                let status = match transfer.submit(cancel.clone()).await {
                                    Ok(TransferStatus::Completed) => vhci::Status::Success,
                                    Ok(TransferStatus::Error) => vhci::Status::Error,
                                    Ok(TransferStatus::TimedOut) => vhci::Status::TimedOut,
                                    Ok(TransferStatus::Cancelled) => vhci::Status::Canceled,
                                    Ok(TransferStatus::Stall) | Err(rusb::Error::InvalidParam) => {
                                        vhci::Status::Stall
                                    }
                                    Ok(TransferStatus::NoDevice) | Err(rusb::Error::NoDevice) => {
                                        vhci::Status::DeviceDisconnected
                                    }
                                    Ok(TransferStatus::Overflow) => vhci::Status::Babble,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "({}) bulk transfer failed on {dev_id:?}", header.seqnum };
                                        vhci::Status::Error
                                    }
                                };

                                let buf = transfer.into_buf().unwrap_or_default();
                                (status, buf, BytesMut::new())
                            }
                            UrbType::Iso => {
                                panic!("I don't want to think about this branch for now");
                                #[repr(transparent)]
                                struct Iso(ioctl::IocIsoPacketData);

                                impl IsoPacket for Iso {
                                    fn len(&self) -> u32 {
                                        self.0.packet_length
                                    }
                                }

                                struct Iter<'a> {
                                    pkts: std::slice::Iter<'a, ioctl::IocIsoPacketData>,
                                }

                                impl Iterator for Iter<'_> {
                                    type Item = Iso;
                                    fn next(&mut self) -> Option<Self::Item> {
                                        self.pkts.next().map(|pkt| Iso(*pkt))
                                    }

                                    fn size_hint(&self) -> (usize, Option<usize>) {
                                        self.pkts.size_hint()
                                    }
                                }

                                impl ExactSizeIterator for Iter<'_> {
                                    fn len(&self) -> usize {
                                        self.pkts.len()
                                    }
                                }

                                // TODO: There might be an issue with setting up
                                //       iso packet offsets, I'm not sure yet.
                                #[allow(unreachable_code)]
                                let num_iso_packets = urb_header.iso_packet_count as usize;
                                let iso_packets =
                                    <[ioctl::IocIsoPacketData]>::ref_from_bytes_with_elems(
                                        &_iso_raw_buf[..],
                                        num_iso_packets,
                                    )
                                    .unwrap();
                                let mut transfer = unsafe {
                                    InnerTransfer::new(num_iso_packets).into_iso(
                                        &handle,
                                        urb_header.endpoint.0,
                                        transfer_buf,
                                        Iter {
                                            pkts: iso_packets.iter(),
                                        },
                                    )
                                };

                                let elapsed = now.elapsed();
                                trace!("({}) took {elapsed:?} to setup {:?} transfer", header.seqnum, urb_header.kind);
                                let status = match transfer.submit(cancel).await {
                                    Ok(TransferStatus::Completed) => vhci::Status::Success,
                                    Ok(TransferStatus::Error) => vhci::Status::Error,
                                    Ok(TransferStatus::TimedOut) => vhci::Status::TimedOut,
                                    Ok(TransferStatus::Cancelled) => vhci::Status::Canceled,
                                    Ok(TransferStatus::Stall) | Err(rusb::Error::InvalidParam) => {
                                        vhci::Status::Stall
                                    }
                                    Ok(TransferStatus::NoDevice) | Err(rusb::Error::NoDevice) => {
                                        vhci::Status::DeviceDisconnected
                                    }
                                    Ok(TransferStatus::Overflow) => vhci::Status::Babble,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "({}) iso transfer failed on {dev_id:?}", header.seqnum };
                                        vhci::Status::Error
                                    }
                                };

                                let iso_packets =
                                    <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                                        &mut _iso_raw_buf[..],
                                        num_iso_packets,
                                    )
                                    .unwrap();
                                for (our_pkt, libusb_pkt) in
                                    iso_packets.iter_mut().zip(transfer.iso_packets().unwrap())
                                {
                                    our_pkt.packet_actual = libusb_pkt.actual_len();
                                    our_pkt.status = libusb_pkt.status() as i32;
                                }

                                let buf = transfer.into_buf().unwrap_or_default();
                                (status, buf, _iso_raw_buf)
                            }
                        };

                        urb_header.status = status;
                        if Dir::Out == urb_header.endpoint.direction()
                        {
                            if transfer.len() != urb_header.actual_transfer_len as usize {
                                warn!("({}) did not finish transferring data", header.seqnum);
                                urb_header.actual_transfer_len = transfer.len() as u16;
                            } else {
                                transfer.clear();
                            }
                        }

                        (header, urb_header, transfer, iso_pkts)
                    }.in_current_span());
                }
                Event::RecvFrame(Some(Recv::PortReset(header))) => {
                    trace!("({}) got port reset", header.seqnum);

                    let handle = Arc::clone(&device);
                    let port_reset = tokio::task::spawn_blocking(move || {
                        port_reset(header.seqnum, dev_id, &handle)
                    });

                    let status = port_reset.await.unwrap();

                    let header = Header {
                        command: msg::Command::RetPort,
                        status,
                        ..header
                    };

                    let mut response = header.as_bytes();
                    // trace!(
                    //     "({}) response is {} bytes",
                    //     header.seqnum,
                    //     response.len()
                    // );
                    tx.write_all_buf(&mut response).await?;
                    if msg::Status::Success != header.status {
                        break Err(Error::ReqFailed);
                    }
                }
                Event::RecvFrame(Some(Recv::Unlink(header))) => {
                    if let Some(transfer) = cancel_tokens.remove(&header.seqnum) {
                        transfer.cancel();
                    }
                }
                Event::SendFrame((header, urb_header, transfer, iso_pkts)) => {
                    cancel_tokens.remove(&header.seqnum);

                    // trace!(
                    //     "({}) {:?} endpoint ({:?}/{}): {:?}, Transfer: {:?}, Iso: {:?}",
                    //     header.seqnum,
                    //     urb_header.kind,
                    //     urb_header.endpoint.direction(),
                    //     urb_header.endpoint.0 & 0x7F,
                    //     urb_header.status,
                    //     &transfer[..],
                    //     &iso_pkts[..]
                    // );

                    let status = match urb_header.status {
                        vhci::Status::Pending => todo!(),
                        vhci::Status::Error => msg::Status::DevErr,
                        vhci::Status::DeviceDisconnected => msg::Status::NoDev,
                        vhci::Status::BitStuff => todo!(),
                        vhci::Status::Crc => todo!(),
                        vhci::Status::NoResponse => todo!(),
                        vhci::Status::BufferUnderrun => todo!(),
                        vhci::Status::BufferOverrun => todo!(),
                        vhci::Status::AllIsoPacketsFailed => todo!(),
                        vhci::Status::ShortPacket => todo!(),
                        vhci::Status::Success | _ => msg::Status::Success,
                        // vhci::Status::Canceled => msg::Status::Success,
                        // vhci::Status::TimedOut => msg::Status::Success,
                        // vhci::Status::DeviceDisabled => msg::Status::Success,
                        // vhci::Status::Stall => msg::Status::Success,
                        // vhci::Status::Babble => msg::Status::Success,
                    };

                    let scratch = [0u8; 7];
                    let padding = {
                        let padded_len = align_to_usize(transfer.len()) - transfer.len();
                        &scratch[..padded_len]
                    };
                    let transfer_padded_len = transfer.len() + padding.len();
                    assert_eq!(transfer_padded_len % 8, 0);

                    let dir = if UrbType::Ctrl == urb_header.kind {
                        urb_header.ctrl_packet.req().dir()
                    } else {
                        urb_header.endpoint.direction()
                    };

                    let actual_transfer_len = if Dir::Out == dir {
                        urb_header.actual_transfer_len
                    } else {
                        transfer.len() as u16
                    };

                    let iso_packet_count = if Dir::Out == dir {
                        urb_header.iso_packet_count
                    } else {
                        (iso_pkts.len() / size_of::<ioctl::IocIsoPacketGiveback>()) as u16
                    };

                    let total_frame_len = if Dir::Out == dir {
                        size_of::<Header>() + size_of::<UrbHeader>()
                    } else {
                        size_of::<Header>()
                            + size_of::<UrbHeader>()
                            + transfer_padded_len
                            + iso_pkts.len()
                    };

                    let header = Header {
                        total_frame_len: (total_frame_len / 8) as u16,
                        command: msg::Command::RetSubmit,
                        status,
                        ..header
                    };

                    let urb = UrbHeader {
                        actual_transfer_len,
                        iso_packet_count,
                        ..urb_header
                    };

                    if msg::Status::Success == header.status {
                        let mut response = header
                            .as_bytes()
                            .chain(urb.as_bytes())
                            .chain(transfer)
                            .chain(padding)
                            .chain(iso_pkts.as_bytes());
                        // trace!(
                        //     "seqnum {} - response is {} bytes",
                        //     header.seqnum,
                        //     response.remaining()
                        // );
                        tx.write_all_buf(&mut response).await?;
                    } else {
                        let _errno = dbg!(io::Error::last_os_error());
                        let mut response = header.as_bytes();
                        // trace!(
                        //     "seqnum {} - response is {} bytes",
                        //     header.seqnum,
                        //     response.len()
                        // );
                        tx.write_all_buf(&mut response).await?;
                        break Err(Error::ReqFailed);
                    }
                }
                Event::RecvFrame(None) => break Ok(()),
            }
        };

        cancel_tokens.into_values().for_each(|token| token.cancel());
        event_handler.cancel();
        drop(device);
        _ = event_handler_loop.join();
        tx.close().await?;

        result
    }
}

pub struct SendDevices<W> {
    tx: W,
}

impl<W> SendDevices<W> {
    pub fn new(tx: W) -> Self {
        Self { tx }
    }
}

impl<W> SendDevices<W>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn resp_list_devices<'a, I, T>(self, iter: impl Fn() -> io::Result<I>) -> Result<()>
    where
        I: Iterator<Item = T>,
        T: msg::SendUsbDeviceInfo,
    {
        use tokio::io::AsyncWriteExt;
        let mut tx = self.tx;

        trace!("getting available devices to send to peer");

        let devices = match iter() {
            Ok(devices) => {
                let mut response = msg::Status::Success.as_bytes().chain(&[0u8; 7][..]);
                tx.write_all_buf(&mut response).await?;
                devices
            }
            Err(err) => {
                let mut response = msg::Status::Failed.as_bytes().chain(&[0u8; 7][..]);
                tx.write_all_buf(&mut response).await?;
                tx.close().await?;
                return Err(err.into());
            }
        };

        for usb in devices {
            let mut device = usb
                .get()
                .as_bytes()
                .chain(usb.interfaces_with_padding().as_bytes());
            tx.write_all_buf(&mut device).await?;
        }

        trace!("sent all devices to peer");

        tx.close().await.map_err(Error::from)
    }
}

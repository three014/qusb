use std::{
    io,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::{Buf, BytesMut};
use nohash_hasher::IntMap;
use proto::{
    data::{Data, ReadError, Ring},
    msg::{self, Header, QusbFrame, UrbFrame, UrbHeader},
};
use rusb::UsbContext;
use rusb_async::{InnerTransfer, TransferStatus};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use vhci::{
    ioctl::{self, UrbType},
    usbfs::{Dir, Recipient, Request},
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
        let mut scratch_buf = [0u8; 2048];
        buf_rx.reserve(4096);

        // Step 1: Connect VHCI port
        let (port, mut work_rx) = vhci
            .register(RegisterPort::Any, DataRate::High)
            .await
            .unwrap();

        enum Event {
            Work(Option<ioctl::Work>),
            UrbResp(io::Result<usize>),
        }

        let seqnum = AtomicU32::new(0);
        let mut addr: u8 = 0xff;
        let mut prev = ioctl::IocPortStat::default();
        let mut handles =
            SimpleMap::<u32, ioctl::UrbHandle>::with_capacity_and_hasher(32, Default::default());
        let mut seqnums =
            SimpleMap::<ioctl::UrbHandle, u32>::with_capacity_and_hasher(32, Default::default());

        info!("starting event loop");

        // Step 3: Start event loop
        'outer: loop {
            debug!("==============================================");
            trace!("waiting for work or data");
            let event = tokio::select! {
                maybe_work = work_rx.recv() => {
                    Event::Work(maybe_work)
                }
                maybe_bytes = buf_rx.fill_with_reader(&mut rx) => {
                    Event::UrbResp(maybe_bytes)
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
                        let next = seqnum.fetch_add(1, Ordering::Relaxed);
                        let handle = ioctl::UrbHandle(u64::MAX);
                        seqnums.insert(handle, next);
                        handles.insert(next, handle);
                        debug!("port is resetting with seqnum {next}");
                        let now = Instant::now();

                        let header = Header {
                            total_frame_len: (size_of::<Header>() / 8) as u16,
                            seqnum: next,
                            command: msg::Command::CmdPort,
                            status: msg::Status::Success,
                        };

                        let elapsed = now.elapsed();
                        let mut request = header.as_bytes();
                        trace!("took {elapsed:?} to setup port reset frame");
                        trace!(
                            "seqnum {} - request is {} bytes",
                            header.seqnum,
                            request.len()
                        );
                        tx.write_all_buf(&mut request).await.unwrap();
                    }
                    if (!prev.flags()).contains(PortFlag::RESUMING)
                        && flags.contains(PortFlag::RESUMING)
                        && status.contains(PortStatus::CONNECTION)
                    {
                        debug!("port is resuming -> completing resume");
                        todo!("do the actual resume thing");
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

                    let mut urb = vhci::UrbWithData::from_ioctl(urb, handle);
                    urb.set_status(vhci::Status::Success);

                    vhci.giveback_urb(urb).await.unwrap();
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle)))) => {
                    assert_eq!(addr, urb.address.get());
                    debug!("got urb with {addr:#x}");
                    let next = seqnum.fetch_add(1, Ordering::Relaxed);
                    assert!(handles.insert(next, handle).is_none());
                    assert!(seqnums.insert(handle, next).is_none());
                    let now = Instant::now();

                    // To ensure 8 byte alignment, we make sure every whole part of the
                    // URB frame is aligned to 8 bytes.
                    let num_isos = urb.packet_count as usize;
                    let transfer_actual_len = urb.buffer_length as usize;
                    let transfer_padded_size = {
                        let buf_and_ctrl_pkt_len =
                            transfer_actual_len + size_of::<ioctl::IocSetupPacket>();
                        align_to_usize(buf_and_ctrl_pkt_len)
                    };

                    let data_len =
                        transfer_padded_size + num_isos * size_of::<ioctl::IocIsoPacketData>();
                    let total_frame_len = size_of::<Header>() + size_of::<UrbHeader>() + data_len;

                    let data = &mut scratch_buf[..data_len];

                    // Grab mutable references for each part of our frame
                    let (transfer, rest) = data.split_at_mut(transfer_padded_size);
                    let iso_data =
                        <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(rest, num_isos)
                            .unwrap();

                    // Write our required data into the slice
                    let header = Header {
                        total_frame_len: (total_frame_len / 8) as u16,
                        seqnum: next,
                        command: msg::Command::CmdSubmit,
                        status: msg::Status::Success,
                    };
                    let mut urb_header = msg::UrbHeader {
                        kind: urb.typ,
                        transfer_actual_len: transfer_actual_len as u16,
                        num_isos: num_isos as u16,
                        interval: urb.interval as u16,
                        flags: urb.flags,
                        endpoint: urb.endpoint,
                        num_errors: 0,
                        status: vhci::Status::Pending,
                    };

                    urb.setup_packet.write_to_prefix(transfer).unwrap();

                    if Dir::Out == urb.endpoint.direction()
                        && (urb.buffer_length > 0 || urb.packet_count > 0)
                    {
                        debug!(
                            "fetching data (transfer_len: {}, iso_packet_len: {})",
                            urb.buffer_length, urb.packet_count
                        );
                        let borrower_urb = UrbWithIsoData {
                            handle,
                            header: &urb_header,
                            transfer: &mut transfer[size_of::<ioctl::IocSetupPacket>()..],
                            iso_data,
                        };

                        vhci.fetch_data(borrower_urb).unwrap();
                    }

                    urb_header.transfer_actual_len += size_of::<ioctl::IocSetupPacket>() as u16;

                    let mut request = header
                        .as_bytes()
                        .chain(urb_header.as_bytes())
                        .chain(data.as_bytes());

                    let elapsed = now.elapsed();
                    trace!("took {:?} to setup URB frame for sending", elapsed);
                    trace!(
                        "seqnum {} - request is {} bytes",
                        header.seqnum,
                        request.remaining()
                    );
                    tx.write_all_buf(&mut request).await.unwrap();
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
                        trace!(
                            "about to write {} bytes into the buffer with seqnum {seqnum}",
                            request.remaining(),
                        );
                        tx.write_all_buf(&mut request).await.unwrap();
                    } else {
                        trace!("URB with {handle:?} had already been returned");
                    }
                }
                Event::UrbResp(Ok(_)) if HEADER_SIZE <= buf_rx.len() => {
                    let header: Header = buf_rx.read().unwrap();
                    debug!("got response with seqnum {}", header.seqnum);

                    match header.status {
                        msg::Status::Success => match header.command {
                            msg::Command::RetSubmit => {
                                let now = Instant::now();
                                let mut min_len = MIN_URB_SUBMIT_SIZE;
                                let frame: &mut UrbFrame = loop {
                                    if buf_rx.fill_until(&mut rx, min_len).await?.is_none() {
                                        break 'outer;
                                    }

                                    match buf_rx.peek_mut_dst() {
                                        Ok(frame) => break frame,
                                        Err(ReadError::CorruptedData) => todo!(),
                                        Err(ReadError::BufferShort {
                                            num_bytes_needed, ..
                                        }) => {
                                            trace!("needs {num_bytes_needed} more bytes while receiving urb frame");
                                            min_len = buf_rx.len() + num_bytes_needed;
                                        }
                                    }
                                };

                                let urb = &mut frame.header;

                                if vhci::Status::Success != urb.status {
                                    warn!("transfer {} failed: {:?}", header.seqnum, urb.status);
                                }
                                let transfer_len = urb.transfer_actual_len as usize;
                                let (transfer, rest) = <[u8]>::mut_from_prefix_with_elems(
                                    &mut frame.data,
                                    align_to_usize(transfer_len),
                                )
                                .unwrap();
                                let iso_packets =
                                    <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                                        rest,
                                        urb.num_isos as usize,
                                    )
                                    .unwrap();

                                let handle = handles.remove(&header.seqnum).unwrap();
                                let _ = seqnums.remove(&handle).unwrap();

                                let lender_urb = UrbWithIsoGiveback {
                                    handle,
                                    header: urb,
                                    transfer,
                                    iso_giveback: iso_packets,
                                };

                                let elapsed = now.elapsed();
                                trace!("took {elapsed:?} to unpack URB for giveback");

                                vhci.giveback_urb(lender_urb).await.unwrap();
                                let size_of_frame = size_of_val(frame);
                                buf_rx.consume(size_of_frame);
                            }
                            // NOTE: This command will eventually be removed because
                            //       the new expectation is for the server to respond
                            //       with the URB regardless.
                            // msg::Command::RetUnlink => {
                            //     if let Some(handle) = handles.remove(&header.seqnum) {
                            //         let _ = seqnums.remove(&handle);
                            //     } else {
                            //         trace!(
                            //             "URB with seqnum {} had already been returned",
                            //             header.seqnum
                            //         );
                            //     }
                            // }
                            msg::Command::RetPort => {
                                debug!("port has been reset");
                                vhci.reset_done(port, true).unwrap();
                            }
                            _ => unreachable!("smh smh smh server"),
                        },
                        msg::Status::Failed => todo!(),
                        msg::Status::DevBusy => todo!(),
                        msg::Status::DevErr => {
                            return Err(io::Error::other("i/o error on remote device").into())
                        }
                        msg::Status::NoDev => {
                            return Err(io::Error::other(
                                "remote device disconnected during borrow",
                            )
                            .into())
                        }
                        msg::Status::Unexpected => todo!(),
                        msg::Status::VersionMismatch => todo!(),
                        msg::Status::Timeout => todo!(),
                        msg::Status::Proto => todo!(),
                    }
                }
                Event::UrbResp(Err(err)) => return Err(err.into()),
                Event::UrbResp(Ok(0)) => break,
                Event::Work(None) => todo!("how did we get here? should we shutdown here?"),
                Event::UrbResp(Ok(num_bytes)) => trace!(
                    "read {num_bytes} bytes and it wasn't enough to decode any msg, continuing..."
                ),
            }
        }

        Ok(())
    }
}

pub enum ServerResp<W, R> {
    ListDevices(SendDevices<W>),
    BorrowDevice(LendDevice<W, R>),
}

const HEADER_SIZE: usize = size_of::<Header>();
const MIN_URB_SUBMIT_SIZE: usize = size_of::<UrbHeader>();

enum LendRecv {
    Urb((Header, Data<UrbFrame>)),
    PortReset(Header),
    Unlink(Header),
}

enum LendEvent {
    RecvFrame(Option<LendRecv>),
    SendFrame((Header, UrbHeader, BytesMut)),
}

async fn recv_frame<R: AsyncRead + Unpin>(
    mut rx: R,
    buf: &mut Ring,
) -> io::Result<Option<LendRecv>> {
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
        msg::Command::CmdSubmit => {
            // In this case this will probably
            // be faster since we already parsed
            // the header
            let header = frame_ref.header.clone();
            let (_, urb) = frame.split::<UrbFrame>();
            Ok(Some(LendRecv::Urb((header, urb))))
        }
        msg::Command::CmdUnlink => Ok(Some(LendRecv::Unlink(frame_ref.header.clone()))),
        msg::Command::CmdPort => Ok(Some(LendRecv::PortReset(frame_ref.header.clone()))),
        _ => unreachable!("client smh smh"),
    }
}

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
    pub async fn lend2(self) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            id: dev_id,
        } = self;
        buf_rx.reserve(2048);

        // TODO: Use global context instead
        let device = match rusb::Context::new()
            .and_then(|ctx| {
                // ctx.set_log_level(LogLevel::Debug);
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
                        handle.claim_interface(interface)?;
                    }
                }
                handle.unconfigure()?;
                Ok(Arc::new(handle))
            }) {
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
                if let Err(err) = ctx.handle_events(Some(Duration::from_secs(500))) {
                    warn!("error from event handler: {err}");
                }
            }
        });

        let mut cancel_tokens: IntMap<u32, CancellationToken> =
            IntMap::with_capacity_and_hasher(256, Default::default());
        let mut transfers: JoinSet<(Header, UrbHeader, BytesMut)> = JoinSet::new();

        let result = loop {
            let check_transfer = !transfers.is_empty();
            match tokio::select! {
                biased;
                frame = recv_frame(&mut rx, &mut buf_rx) => {
                    LendEvent::RecvFrame(frame?)
                }
                result = transfers.join_next(), if check_transfer => {
                    LendEvent::SendFrame(result.unwrap().unwrap())
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => continue,
            } {
                LendEvent::RecvFrame(Some(LendRecv::Urb((header, urb_frame)))) => {
                    let cancel = CancellationToken::new();
                    cancel_tokens.insert(header.seqnum, cancel.clone());

                    let handle = Arc::clone(&device);
                    transfers.spawn(async move {
                        let (urb_header, data) = urb_frame.split::<[u8]>();
                        let mut urb_header = urb_header.read();
                        let mut data = data.into_bytes_mut();
                        let transfer_len = urb_header.transfer_actual_len as usize;
                        let mut transfer_buf = data.split_to(align_to_usize(transfer_len));
                        let ctrl =
                            ioctl::IocSetupPacket::ref_from_bytes(&transfer_buf[..8]).unwrap();

                        match urb_header.kind {
                            UrbType::Ctrl
                                if Request::STANDARD_DEVICE_SET_CONFIGURATION == ctrl.req() =>
                            {
                                trace! { %ctrl };
                                let desired = (ctrl.value() & 0xFF) as u8;
                                urb_header.status = if handle
                                    .device()
                                    .active_config_descriptor()
                                    .is_ok_and(|config| desired == config.number())
                                {
                                    debug!("config {desired} is already set");
                                    vhci::Status::Success
                                } else {
                                    match handle.set_active_configuration(desired) {
                                        Ok(_) => {
                                            debug!("set config {desired}");
                                            vhci::Status::Success
                                        }
                                        Err(err) => {
                                            warn! { %err, "couldn't set configuration" };
                                            vhci::Status::Stall
                                        }
                                    }
                                };
                                transfer_buf.clear();

                                (header, urb_header, transfer_buf)
                            }
                            UrbType::Ctrl => {
                                trace! { %ctrl };
                                assert!(transfer_len >= 8);
                                let (mut interface, mut alternate) =
                                    if Request::STANDARD_INTERFACE_GET_INTERFACE == ctrl.req() {
                                        let interface = (ctrl.index() & 0xFF) as u8;
                                        let alternate = transfer_buf
                                            .get(size_of::<ioctl::IocSetupPacket>())
                                            .copied()
                                            .unwrap_or_default();

                                        if interface != alternate {
                                            (Some(interface), Some(alternate))
                                        } else {
                                            (Some(interface), None)
                                        }
                                    } else if Recipient::Interface == ctrl.req().recipient() {
                                        (Some((ctrl.index() & 0xFF) as u8), None)
                                    } else {
                                        (None, None)
                                    };

                                if let Some(int) = interface.take_if(|int| {
                                    handle
                                        .kernel_driver_active(*int)
                                        .is_ok_and(|is_active| true == is_active)
                                }) {
                                    handle.claim_interface(int).unwrap();
                                    debug!("successfully claimed interface {int}");
                                }

                                if let Some(alt) = alternate.take_if(|int| {
                                    handle
                                        .kernel_driver_active(*int)
                                        .is_ok_and(|is_active| true == is_active)
                                }) {
                                    handle.claim_interface(alt).unwrap();
                                    debug!("successfully claimed interface {alt}");
                                }

                                let w_length = ctrl.length();
                                assert_eq!(w_length + 8, transfer_len as u16);
                                let buf = transfer_buf.split_to(transfer_len);
                                // SAFETY: Transfer buffer is longer than
                                //         required lengths, and setup packet
                                //         contains the right length as well.
                                let mut transfer = unsafe {
                                    InnerTransfer::new(0).into_ctrl(
                                        &handle,
                                        buf,
                                        Duration::from_millis(900),
                                    )
                                };

                                urb_header.status = match transfer.submit(cancel).await {
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
                                    Ok(TransferStatus::Overflow) => vhci::Status::BufferOverrun,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "ctrl transfer failed on {dev_id:?}"};
                                        vhci::Status::Error
                                    }
                                };

                                let buf = transfer
                                    .into_buf()
                                    .unwrap_or_default()
                                    .split_off(size_of::<ioctl::IocSetupPacket>());
                                (header, urb_header, buf)
                            }
                            UrbType::Int => {
                                let transfer_len =
                                    transfer_len - size_of::<ioctl::IocSetupPacket>();
                                let buf = transfer_buf
                                    .split_off(size_of::<ioctl::IocSetupPacket>())
                                    .split_to(transfer_len);
                                let mut transfer = unsafe {
                                    InnerTransfer::new(0).into_int(
                                        &handle,
                                        urb_header.endpoint.0,
                                        buf,
                                    )
                                };

                                urb_header.status = match transfer.submit(cancel).await {
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
                                    Ok(TransferStatus::Overflow) => vhci::Status::BufferOverrun,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "int transfer failed on {dev_id:?}"};
                                        vhci::Status::Error
                                    }
                                };

                                let buf = transfer.into_buf().unwrap_or_default();
                                (header, urb_header, buf)
                            }
                            UrbType::Bulk => {
                                let transfer_len =
                                    transfer_len - size_of::<ioctl::IocSetupPacket>();
                                let buf = transfer_buf
                                    .split_off(size_of::<ioctl::IocSetupPacket>())
                                    .split_to(transfer_len);
                                let mut transfer = unsafe {
                                    InnerTransfer::new(0).into_bulk(
                                        &handle,
                                        urb_header.endpoint.0,
                                        buf,
                                    )
                                };

                                urb_header.status = match transfer.submit(cancel).await {
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
                                    Ok(TransferStatus::Overflow) => vhci::Status::BufferOverrun,
                                    Err(rusb::Error::Busy) => {
                                        unreachable!("for now, no transfer can be resubmitted")
                                    }
                                    Err(rusb::Error::NotSupported) => {
                                        unreachable!("will we ever mess with the transfer flags?")
                                    }
                                    Err(err) => {
                                        warn! { %err, "int transfer failed on {dev_id:?}"};
                                        vhci::Status::Error
                                    }
                                };

                                let buf = transfer.into_buf().unwrap_or_default();
                                (header, urb_header, buf)
                            }
                            UrbType::Iso => todo!(),
                        }
                    });
                }
                LendEvent::RecvFrame(Some(LendRecv::PortReset(header))) => {
                    trace!("got port reset");

                    let status = match device.reset() {
                        Ok(_) => {
                            trace!("port has been reset");
                            msg::Status::Success
                        }
                        Err(err) => {
                            error! { %err, "error while unconfiguring device {dev_id:?}" };
                            msg::Status::DevErr
                        }
                    };

                    let header = Header {
                        command: msg::Command::RetPort,
                        status,
                        ..header
                    };

                    let mut response = header.as_bytes();
                    trace!(
                        "seqnum {} - response is {} bytes",
                        header.seqnum,
                        response.len()
                    );
                    tx.write_all_buf(&mut response).await?;
                    if msg::Status::Success != header.status {
                        tx.close().await?;
                        break Err(Error::ReqFailed);
                    }
                }
                LendEvent::RecvFrame(Some(LendRecv::Unlink(header))) => {
                    if let Some(transfer) = cancel_tokens.remove(&header.seqnum) {
                        transfer.cancel();
                    }
                }
                LendEvent::SendFrame((header, urb_header, mut data)) => {
                    cancel_tokens.remove(&header.seqnum);

                    if Dir::Out == urb_header.endpoint.direction() && data.len() == data.capacity()
                    {
                        data.clear();
                    }
                    trace!(
                        "Seq {:?} on {:?} endpoint ({:?}/{}): {:?}, Data: {:?}",
                        header.seqnum,
                        urb_header.kind,
                        urb_header.endpoint.direction(),
                        urb_header.endpoint.0 & 0x7F,
                        urb_header.status,
                        &data[..]
                    );

                    let scratch = [0u8; 7];
                    let transfer = &data[..];
                    let padding = {
                        let padded_len = align_to_usize(transfer.len()) - transfer.len();
                        &scratch[..padded_len]
                    };

                    let transfer_padded_len = transfer.len() + padding.len();
                    assert_eq!(transfer_padded_len % 8, 0);

                    // TODO: Add in ISO packet length + packets
                    let total_frame_len =
                        size_of::<Header>() + size_of::<UrbHeader>() + transfer_padded_len;

                    let status = match urb_header.status {
                        vhci::Status::Pending => todo!(),
                        vhci::Status::Error => msg::Status::DevErr,
                        vhci::Status::DeviceDisconnected => msg::Status::NoDev,
                        vhci::Status::BitStuff => todo!(),
                        vhci::Status::Crc => todo!(),
                        vhci::Status::NoResponse => todo!(),
                        vhci::Status::Babble => todo!(),
                        vhci::Status::BufferUnderrun => todo!(),
                        vhci::Status::AllIsoPacketsFailed => todo!(),
                        vhci::Status::ShortPacket => todo!(),
                        vhci::Status::Success | _ => msg::Status::Success,
                        // vhci::Status::Canceled => msg::Status::Success,
                        // vhci::Status::TimedOut => msg::Status::Success,
                        // vhci::Status::DeviceDisabled => msg::Status::Success,
                        // vhci::Status::Stall => msg::Status::Success,
                        // vhci::Status::BufferOverrun => msg::Status::Success,
                    };
                    let header = Header {
                        total_frame_len: (total_frame_len / 8) as u16,
                        command: msg::Command::RetSubmit,
                        status,
                        ..header
                    };

                    let urb = UrbHeader {
                        transfer_actual_len: transfer.len() as u16,
                        num_isos: 0,
                        ..urb_header
                    };

                    if msg::Status::Success == header.status {
                        let mut response = header
                            .as_bytes()
                            .chain(urb.as_bytes())
                            .chain(transfer)
                            .chain(padding);
                        // .chain(iso_packets.as_bytes());
                        trace!(
                            "seqnum {} - response is {} bytes",
                            header.seqnum,
                            response.remaining()
                        );
                        tx.write_all_buf(&mut response).await?;
                    } else {
                        let _errno = dbg!(io::Error::last_os_error());
                        let mut response = header.as_bytes();
                        trace!(
                            "seqnum {} - response is {} bytes",
                            header.seqnum,
                            response.len()
                        );
                        tx.write_all_buf(&mut response).await?;
                        tx.close().await?;
                        break Err(Error::ReqFailed);
                    }

                    // Don't let ring buffer reuse the data
                    data.advance(data.len());
                }
                LendEvent::RecvFrame(None) => break Ok(()),
            }
        };

        event_handler.cancel();
        drop(device);
        _ = event_handler_loop.join();

        result
    }

    // #[tracing::instrument(skip_all, level = "trace")]
    // pub async fn lend(self) -> Result<()> {
    //     let Self {
    //         mut tx,
    //         mut rx,
    //         mut buf_rx,
    //         id: dev_id,
    //     } = self;

    //     let handle = match rusb::Context::new()
    //         .and_then(|ctx| {
    //             // ctx.set_log_level(LogLevel::Debug);
    //             ctx.devices()
    //         })
    //         .and_then(|list| {
    //             list.iter()
    //                 .find(|dev| {
    //                     dev_id.bus_number == dev.bus_number() && dev_id.device_addr == dev.address()
    //                 })
    //                 .ok_or(rusb::Error::NoDevice)
    //         })
    //         .and_then(|dev| dev.open())
    //         .and_then(|handle| {
    //             handle.set_auto_detach_kernel_driver(true)?;
    //             for interface in 0..=16 {
    //                 if let Ok(true) = handle.kernel_driver_active(interface) {
    //                     handle.detach_kernel_driver(interface)?;
    //                 }
    //             }
    //             handle.unconfigure()?;
    //             Ok(handle)
    //         }) {
    //         Ok(handle) => {
    //             let mut response = msg::Status::Success.as_bytes().chain(&[0u8; 7][..]);
    //             tx.write_all_buf(&mut response).await?;
    //             handle
    //         }
    //         Err(err) => {
    //             let err = RusbError { kind: err, dev_id };
    //             let ret_err = Error::from(err);
    //             let status = msg::Status::from(err);

    //             let mut response = status.as_bytes().chain(&[0u8; 7][..]);

    //             tx.write_all_buf(&mut response).await.unwrap();
    //             tx.close().await.unwrap();
    //             return Err(ret_err);
    //         }
    //     };

    //     const HEADER_SIZE: usize = size_of::<Header>();
    //     const MIN_URB_SUBMIT_SIZE: usize = size_of::<UrbHeader>();

    //     trace!("starting lender loop");
    //     'outer: loop {
    //         if buf_rx.fill_until(&mut rx, HEADER_SIZE).await?.is_none() {
    //             break 'outer;
    //         }

    //         let mut header: Header = buf_rx.read().unwrap();

    //         // Parse request big-time
    //         match header.command {
    //             msg::Command::CmdSubmit => {
    //                 trace!("got urb submit");

    //                 let mut min_len = MIN_URB_SUBMIT_SIZE;
    //                 let frame: &mut UrbFrame = loop {
    //                     if buf_rx.fill_until(&mut rx, min_len).await?.is_none() {
    //                         break 'outer;
    //                     }

    //                     match buf_rx.peek_mut_dst() {
    //                         Ok(frame) => break frame,
    //                         Err(ReadError::CorruptedData) => todo!(),
    //                         Err(ReadError::BufferShort {
    //                             num_bytes_needed, ..
    //                         }) => {
    //                             min_len = buf_rx.len() + num_bytes_needed;
    //                         }
    //                     }
    //                 };

    //                 let dbg = false;
    //                 let urb = if dbg {
    //                     dbg!(&mut frame.header)
    //                 } else {
    //                     &mut frame.header
    //                 };
    //                 let mut transfer_len = urb.transfer_actual_len as usize;
    //                 let (transfer, rest) = <[u8]>::mut_from_prefix_with_elems(
    //                     &mut frame.data,
    //                     align_to_usize(transfer_len),
    //                 )
    //                 .unwrap();
    //                 let iso_packets = <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(
    //                     rest,
    //                     usize::from(urb.num_isos),
    //                 )
    //                 .unwrap();
    //                 let (ctrl, transfer) =
    //                     ioctl::IocSetupPacket::try_mut_from_prefix(transfer).unwrap();
    //                 transfer_len -= size_of::<ioctl::IocSetupPacket>();

    //                 let status = match (urb.kind, urb.endpoint.direction()) {
    //                     (UrbType::Ctrl, Dir::In) => {
    //                         match handle.read_control(
    //                             ctrl.bm_request_type,
    //                             ctrl.b_request as u8,
    //                             ctrl.w_value,
    //                             ctrl.w_index,
    //                             &mut transfer[..transfer_len],
    //                             Duration::from_millis(600),
    //                         ) {
    //                             Ok(bytes_written) => {
    //                                 urb.transfer_actual_len = bytes_written as u16;
    //                                 vhci::Status::Success
    //                             }
    //                             Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
    //                             Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
    //                             Err(rusb::Error::Io) => vhci::Status::Error,
    //                             Err(rusb::Error::Overflow) => vhci::Status::BufferOverrun,
    //                             Err(err) => {
    //                                 warn! { %err, "couldn't read from ctrl" };
    //                                 vhci::Status::Stall
    //                             }
    //                         }
    //                     }
    //                     (UrbType::Ctrl, Dir::Out)
    //                         if ctrl.req().is_some_and(|req| {
    //                             STANDARD_DEVICE_SET_CONFIGURATION == (ctrl.request_type(), req)
    //                         }) =>
    //                     {
    //                         if dbg {
    //                             dbg!((ctrl.request_type(), ctrl.req()));
    //                             dbg!((ctrl.value(), ctrl.index(), ctrl.length()));
    //                         }
    //                         let desired = (ctrl.value() & 0xFF) as u8;
    //                         if handle
    //                             .device()
    //                             .active_config_descriptor()
    //                             .is_ok_and(|config| desired == config.number())
    //                         {
    //                             vhci::Status::Success
    //                         } else {
    //                             match handle.set_active_configuration(desired) {
    //                                 Ok(_) => {
    //                                     urb.transfer_actual_len = 0;
    //                                     vhci::Status::Success
    //                                 }
    //                                 Err(err) => {
    //                                     warn! { %err, "couldn't set configuration" };
    //                                     vhci::Status::Stall
    //                                 }
    //                             }
    //                         }
    //                     }
    //                     (UrbType::Ctrl, Dir::Out) => {
    //                         if dbg {
    //                             dbg!((ctrl.request_type(), ctrl.req()));
    //                             dbg!((ctrl.value(), ctrl.index(), ctrl.length()));
    //                         }
    //                         if Some(Req::GetInterface) == ctrl.req() {
    //                             let interface = (ctrl.index() & 0xFF) as u8;
    //                             let _ = handle.detach_kernel_driver(interface);
    //                             handle.claim_interface(interface).unwrap();
    //                             trace!("successfully claimed interface {interface}");
    //                             let alternate = transfer.get(0).copied().unwrap_or_default();
    //                             if interface != alternate {
    //                                 let _ = handle.detach_kernel_driver(alternate);
    //                                 handle.claim_interface(alternate).unwrap();
    //                                 trace!("successfully claimed interface {alternate}");
    //                             }
    //                         }
    //                         match handle.write_control(
    //                             ctrl.bm_request_type,
    //                             ctrl.b_request as u8,
    //                             ctrl.w_value,
    //                             ctrl.w_index,
    //                             &mut transfer[..transfer_len],
    //                             Duration::from_millis(600),
    //                         ) {
    //                             Ok(_) => {
    //                                 urb.transfer_actual_len = 0;
    //                                 vhci::Status::Success
    //                             }
    //                             Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
    //                             Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
    //                             Err(rusb::Error::Io) => vhci::Status::Error,
    //                             Err(err) => {
    //                                 warn! { %err, "couldn't write to control" };
    //                                 vhci::Status::Stall
    //                             }
    //                         }
    //                     }
    //                     (UrbType::Iso, _) => vhci::Status::Stall,
    //                     (UrbType::Int, Dir::In) => {
    //                         match handle.read_interrupt(
    //                             urb.endpoint.0,
    //                             &mut transfer[..transfer_len],
    //                             Duration::from_secs(4),
    //                         ) {
    //                             Ok(bytes_written) => {
    //                                 urb.transfer_actual_len = bytes_written as u16;
    //                                 vhci::Status::Success
    //                             }
    //                             Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
    //                             Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
    //                             Err(rusb::Error::Overflow) => vhci::Status::BufferOverrun,
    //                             Err(rusb::Error::Io) => vhci::Status::Error,
    //                             Err(err) => {
    //                                 warn! { %err, "couldn't read interrupt" };
    //                                 vhci::Status::Stall
    //                             }
    //                         }
    //                     }
    //                     (UrbType::Int, Dir::Out) => todo!(),
    //                     (UrbType::Bulk, Dir::In) => todo!(),
    //                     (UrbType::Bulk, Dir::Out) => todo!(),
    //                 };

    //                 if vhci::Status::Success != status {
    //                     urb.transfer_actual_len = 0;
    //                 }

    //                 urb.status = status;

    //                 header.command = msg::Command::RetSubmit;
    //                 header.status = match status {
    //                     vhci::Status::Pending => todo!(),
    //                     vhci::Status::Error => msg::Status::DevErr,
    //                     // vhci::Status::DeviceDisabled => {
    //                     //     todo!()
    //                     // }
    //                     vhci::Status::DeviceDisconnected => msg::Status::NoDev,
    //                     vhci::Status::BitStuff => todo!(),
    //                     vhci::Status::Crc => todo!(),
    //                     vhci::Status::NoResponse => todo!(),
    //                     vhci::Status::Babble => todo!(),
    //                     vhci::Status::BufferUnderrun => todo!(),
    //                     vhci::Status::AllIsoPacketsFailed => todo!(),
    //                     vhci::Status::ShortPacket => todo!(),
    //                     vhci::Status::Canceled => todo!(),
    //                     vhci::Status::Success | _ => msg::Status::Success,
    //                 };

    //                 if msg::Status::Success == header.status {
    //                     let mut response = header
    //                         .as_bytes()
    //                         .chain(urb.as_bytes())
    //                         .chain(&transfer[..align_to_usize(urb.transfer_actual_len as usize)])
    //                         .chain(iso_packets.as_bytes());
    //                     trace!(
    //                         "seqnum {} - response is {} bytes",
    //                         header.seqnum,
    //                         response.remaining()
    //                     );
    //                     tx.write_all_buf(&mut response).await?;
    //                 } else {
    //                     let _errno = dbg!(io::Error::last_os_error());
    //                     let mut response = header.as_bytes();
    //                     trace!(
    //                         "seqnum {} - response is {} bytes",
    //                         header.seqnum,
    //                         response.len()
    //                     );
    //                     tx.write_all_buf(&mut response).await?;
    //                     tx.close().await?;
    //                     return Err(Error::ReqFailed);
    //                 }

    //                 let size_of_frame = size_of_val(frame);
    //                 buf_rx.consume(size_of_frame);
    //             }
    //             msg::Command::CmdUnlink => {
    //                 trace!("got urb unlink");

    //                 // TODO: If we ever figure out asynchronous transfer,
    //                 //       then implement this for reals

    //                 header.command = msg::Command::RetUnlink;
    //                 header.status = msg::Status::Success;
    //                 let mut response = header.as_bytes();
    //                 trace!(
    //                     "seqnum {} - response is {} bytes",
    //                     header.seqnum,
    //                     response.len()
    //                 );
    //                 tx.write_all_buf(&mut response).await?;
    //             }
    //             msg::Command::CmdPort => {
    //                 trace!("got port reset");

    //                 header.status = match handle.reset() {
    //                     Ok(_) => {
    //                         trace!("port has been reset");
    //                         msg::Status::Success
    //                     }
    //                     Err(err) => {
    //                         error! { %err, "error while unconfiguring device {dev_id:?}" };
    //                         msg::Status::DevErr
    //                     }
    //                 };

    //                 header.command = msg::Command::RetPort;

    //                 // let mut response = dbg!(&header).as_bytes();
    //                 let mut response = header.as_bytes();
    //                 trace!(
    //                     "seqnum {} - response is {} bytes",
    //                     header.seqnum,
    //                     response.len()
    //                 );
    //                 tx.write_all_buf(&mut response).await?;
    //                 if msg::Status::Success != header.status {
    //                     tx.close().await?;
    //                     return Err(Error::ReqFailed);
    //                 }
    //             }
    //             _ => unreachable!("smh smh client"),
    //         }
    //     }
    //     todo!()
    // }
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

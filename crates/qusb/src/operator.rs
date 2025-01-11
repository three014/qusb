use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, BytesMut};
use proto::{
    data::{ReadError, Ring},
    msg::{
        self, Header, IsoPacketData, IsoPacketGiveback, IsoPacketHeader, TransferHeader, UrbFrame,
        UrbHeader,
    },
};
use rusb::UsbContext;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, trace, warn};
use vhci::{
    ioctl::{self, UrbType},
    usbfs::{Dir, Req, STANDARD_DEVICE_SET_ADDRESS, STANDARD_DEVICE_SET_CONFIGURATION},
    DataRate, PortChange, PortFlag, PortStatus,
};
use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    stub::{self, RegisterPort},
    utils::{self, SimpleMap},
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
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        vhci: stub::Controller,
        id: msg::UsbDeviceId,
    ) -> Self {
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
            id: dev_id,
        } = self;
        let mut buf_tx = BytesMut::with_capacity(1024);

        // Step 1: Connect VHCI port
        let (port, mut work_rx) = vhci
            .register(RegisterPort::Any, DataRate::Full)
            .await
            .unwrap();

        enum Event {
            Work(Option<ioctl::Work>),
            UrbResp(io::Result<usize>),
        }

        // Step 2: Setup context
        const HEADER_SIZE: usize = size_of::<Header>();
        const MIN_URB_SUBMIT_SIZE: usize =
            size_of::<UrbHeader>() + size_of::<TransferHeader>() + size_of::<IsoPacketHeader>();

        let seqnum = AtomicU64::new(0);
        let mut addr: u8 = 0xff;
        let mut prev = ioctl::IocPortStat::default();
        let mut handles =
            SimpleMap::<u64, ioctl::UrbHandle>::with_capacity_and_hasher(32, Default::default());
        let mut seqnums =
            SimpleMap::<ioctl::UrbHandle, u64>::with_capacity_and_hasher(32, Default::default());

        trace!("starting event loop");

        // Step 3: Start event loop
        'outer: loop {
            let event = tokio::select! {
                maybe_work = work_rx.recv() => {
                    Event::Work(maybe_work)
                }
                maybe_bytes = buf_rx.fill_with_reader(&mut rx) => {
                    Event::UrbResp(maybe_bytes)
                }
            };

            debug!("==============================================");
            let now = Instant::now();
            match event {
                Event::Work(Some(ioctl::Work::PortStat(next))) => {
                    debug!("got port stat for {:?}", port);
                    // debug!("status: {:?}", next.status());
                    // debug!("change: {:?}", next.change());
                    // debug!("index: {:?}", next.index());
                    // debug!("flags: {:?}", next.flags());
                    if next.change().contains(PortChange::CONNECTION) {
                        trace!("CONNECTION state changed -> invalidating address");
                        addr = 0xff;
                    }
                    if next.change().contains(PortChange::RESET)
                        && (!next.status()).contains(PortStatus::RESET)
                        && next.status().contains(PortStatus::ENABLE)
                    {
                        trace!("RESET successful -> use default address");
                        addr = 0;
                    }
                    if prev.status().contains(PortStatus::POWER)
                        && (!next.status()).contains(PortStatus::POWER)
                    {
                        trace!("port is powered off");
                    }
                    if (!prev.status()).contains(PortStatus::RESET)
                        && next
                            .status()
                            .contains(PortStatus::RESET | PortStatus::CONNECTION)
                    {
                        trace!("port is resetting");
                        let next = seqnum.fetch_add(1, Ordering::Relaxed);
                        let handle = ioctl::UrbHandle(u64::MAX);
                        seqnums.insert(handle, next);
                        handles.insert(next, handle);

                        let reserve_size = size_of::<Header>();

                        buf_tx.clear();
                        buf_tx.reserve(reserve_size.saturating_sub(buf_tx.capacity()));
                        buf_tx.put_bytes(0, reserve_size);

                        let reserved_chunk = &mut buf_tx[..reserve_size];

                        // Grab mutable references for each part of our frame
                        let header = Header::try_mut_from_bytes(reserved_chunk).unwrap();
                        *header = Header {
                            seqnum: next,
                            dev_id,
                            command: msg::Command::CmdPort,
                            status: msg::Status::Success,
                            reset: true,
                            _padding: Default::default(),
                        };

                        tx.write_all_buf(&mut &buf_tx[..reserve_size])
                            .await
                            .unwrap();
                    }
                    if (!prev.flags()).contains(PortFlag::RESUMING)
                        && next.flags().contains(PortFlag::RESUMING)
                        && next.status().contains(PortStatus::CONNECTION)
                    {
                        trace!("port is resuming -> completing resume");
                        todo!("do the actual resume thing");
                    }
                    prev = next;
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle))))
                    if UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && STANDARD_DEVICE_SET_ADDRESS
                            == (urb.setup_packet.request_type(), urb.setup_packet.req()) =>
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
                    let now = Instant::now();
                    let next = seqnum.fetch_add(1, Ordering::Relaxed);
                    assert!(handles.insert(next, handle).is_none());
                    assert!(seqnums.insert(handle, next).is_none());

                    // TODO: Do we reclaim the reserved chunk automatically??
                    //       - Seems like we gotta manually give back the chunk
                    //       - Figure that out once we have a working system.
                    //         It doesn't seem like it'll take took long to change
                    //         if we're wrong.
                    // TODO: Decide whether we really want to use a "ring buffer":
                    //       - Different urb-tasks can claim a chunk of data and fill them
                    //         with their data, then give them back to the ring buffer
                    //       - The ring buffer sends out a chunk of data after a certain
                    //         threshold or interval has been reached
                    //       - Ring buffer must become multi-threaded and therefore must
                    //         start using sync methods
                    //       If we don't then a simpler scratch buffer would work just fine
                    //
                    //       Decision for now: Use an "arena"; stick to in-order behavior

                    // To ensure 8 byte alignment, we make sure every whole part of the
                    // URB frame is aligned to 8 bytes.
                    let transfer_padded_size = usize::try_from(urb.buffer_length)
                        .map(|len| len.next_multiple_of(8))
                        .unwrap_or_default();

                    // We then calculate the total size of the URB frame...
                    let reserve_size = size_of::<Header>()
                        + size_of::<msg::UrbHeader>()
                        + size_of::<msg::TransferHeader>()
                        + transfer_padded_size
                        + size_of::<msg::IsoPacketHeader>()
                        + usize::try_from(urb.packet_count).unwrap()
                            * size_of::<ioctl::IocIsoPacketData>();

                    // ...and make sure we have enough space in our scratch buffer
                    buf_tx.clear();
                    buf_tx.reserve(reserve_size.saturating_sub(buf_tx.capacity()));
                    buf_tx.put_bytes(0, reserve_size);

                    // By this point `reserved_chunk` should be a zero-initialized
                    // slice of u8's ready for writing our data
                    let reserved_chunk = &mut buf_tx[..reserve_size];

                    // Grab mutable references for each part of our frame
                    let (header, rest) = Header::try_mut_from_prefix(reserved_chunk).unwrap();
                    let (urb_header, rest) = msg::UrbHeader::try_mut_from_prefix(rest).unwrap();
                    let (transfer, rest) =
                        msg::Transfer::try_mut_from_prefix_with_elems(rest, transfer_padded_size)
                            .unwrap();
                    let iso = msg::IsoPacketData::try_mut_from_bytes_with_elems(
                        rest,
                        urb.packet_count.try_into().unwrap(),
                    )
                    .unwrap();

                    // Write our required data into the slice
                    *header = Header {
                        seqnum: next,
                        dev_id,
                        command: msg::Command::CmdSubmit,
                        status: msg::Status::Success,
                        reset: false,
                        _padding: Default::default(),
                    };
                    *urb_header = msg::UrbHeader {
                        setup_packet: urb.setup_packet,
                        interval: urb.interval.try_into().unwrap(),
                        flags: urb.flags,
                        address: urb.address,
                        endpoint: urb.endpoint,
                        kind: urb.typ,
                        num_errors: 0,
                        status: vhci::Status::Pending,
                        _padding: Default::default(),
                    };
                    transfer.header = msg::TransferHeader {
                        aligned_len: transfer_padded_size.try_into().unwrap(),
                        actual_len: urb.buffer_length.try_into().unwrap(),
                        _padding: Default::default(),
                    };
                    iso.header = msg::IsoPacketHeader {
                        len: urb.packet_count.try_into().unwrap(),
                        _padding: Default::default(),
                    };

                    if Dir::Out == urb.endpoint.direction()
                        && (urb.buffer_length > 0 || urb.packet_count > 0)
                    {
                        trace!(
                            "fetching data (transfer_len: {}, iso_packet_len: {})",
                            urb.buffer_length,
                            urb.packet_count
                        );
                        let borrower_urb = UrbWithIsoData {
                            handle,
                            header: urb_header,
                            transfer: transfer.get_mut(),
                            iso_data: iso.get_mut(),
                        };

                        vhci.fetch_data(borrower_urb).unwrap();
                    }

                    let dur = now.elapsed();
                    if Duration::from_micros(15) < dur {
                        trace!("took {:?} to setup URB frame for sending", dur);
                    }

                    // trace!(
                    //     "about to write {} bytes to buffer with seqnum {}",
                    //     buf_tx.len(),
                    //     next
                    // );

                    // And away we go!!!!
                    tx.write_all_buf(&mut &buf_tx[..reserve_size])
                        .await
                        .unwrap();
                }
                Event::Work(Some(ioctl::Work::CancelUrb(handle))) => {
                    debug!("got cancel urb with {handle:?}");
                    if let Some(&seqnum) = seqnums.get(&handle) {
                        assert!(
                            handles.remove(&seqnum).is_some(),
                            "tried to remove seqnum {seqnum}"
                        );

                        // TODO: Once we get async usb transfer working,
                        //       we can send cancel requests. For now,
                        //       we can just break here.
                        continue;

                        let reserve_size = size_of::<Header>();

                        buf_tx.clear();
                        buf_tx.reserve(reserve_size.saturating_sub(buf_tx.capacity()));
                        buf_tx.put_bytes(0, reserve_size);
                        let reserved_chunk = &mut buf_tx[..reserve_size];

                        let header = Header::try_mut_from_bytes(reserved_chunk).unwrap();

                        *header = Header {
                            seqnum,
                            dev_id,
                            command: msg::Command::CmdUnlink,
                            status: msg::Status::Success,
                            reset: false,
                            _padding: Default::default(),
                        };

                        trace!(
                            "about to write {} bytes into the buffer with seqnum {seqnum}",
                            buf_tx.len(),
                        );
                        tx.write_all_buf(&mut buf_tx).await.unwrap();
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
                                if let Some(handle) = handles.remove(&header.seqnum) {
                                    let _ = seqnums.remove(&handle).unwrap();

                                    let mut min_len = MIN_URB_SUBMIT_SIZE;
                                    let frame: &mut UrbFrame = loop {
                                        if buf_rx.fill_until(&mut rx, min_len).await?.is_none() {
                                            break 'outer;
                                        }

                                        match buf_rx.peek_mut_dst() {
                                            Ok(frame) => break frame,
                                            Err(ReadError::CorruptedData) => todo!(),
                                            Err(ReadError::BufferShort {
                                                num_bytes_needed,
                                                ..
                                            }) => {
                                                min_len = buf_rx.len() + num_bytes_needed;
                                            }
                                        }
                                    };
                                    let dbg = true;
                                    let urb = if dbg {
                                        dbg!(&mut frame.header)
                                    } else {
                                        &mut frame.header
                                    };
                                    let (transfer_header, rest) =
                                        TransferHeader::mut_from_prefix(&mut frame.data).unwrap();
                                    if dbg {
                                        dbg!(&transfer_header);
                                    }
                                    let (transfer, rest) = <[u8]>::mut_from_prefix_with_elems(
                                        rest,
                                        usize::from(transfer_header.aligned_len),
                                    )
                                    .unwrap();
                                    let iso_packets =
                                        IsoPacketGiveback::try_mut_from_bytes(rest).unwrap();

                                    let dur = now.elapsed();
                                    if Duration::from_micros(15) < dur {
                                        trace!("took {dur:?} to unpack URB for giveback");
                                    }
                                    let lender_urb = UrbWithIsoGiveback {
                                        handle,
                                        header: urb,
                                        transfer: &mut transfer
                                            [..usize::from(transfer_header.actual_len)],
                                        iso_giveback: iso_packets.get_mut(),
                                    };

                                    vhci.giveback_urb(lender_urb).await.unwrap();
                                    let size_of_frame = size_of_val(frame);
                                    buf_rx.consume(size_of_frame);
                                } else {
                                    trace!(
                                        "URB with seqnum {} had already been dropped",
                                        header.seqnum
                                    );
                                }
                            }
                            msg::Command::RetUnlink => {
                                if let Some(handle) = handles.remove(&header.seqnum) {
                                    let _ = seqnums.remove(&handle);
                                } else {
                                    trace!(
                                        "URB with seqnum {} had already been returned",
                                        header.seqnum
                                    );
                                }
                            }
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
                _ => (),
            }
        }

        Ok(())
    }
}

pub enum ServerResp<W, R> {
    ListDevices(SendDevices<W>),
    BorrowDevice(LendDevice<W, R>),
}

pub struct LendDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    id: msg::UsbDeviceId,
}

impl<W, R> LendDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        id: msg::UsbDeviceId,
    ) -> Self {
        Self {
            tx,
            rx,
            buf_rx,
            id,
        }
    }
}

impl<W, R> LendDevice<W, R>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
    R: AsyncRead + Unpin,
{
    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn lend(self) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            id: dev_id,
        } = self;

        let handle = match rusb::Context::new()
            .and_then(|mut ctx| {
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
                        handle.detach_kernel_driver(interface)?;
                    }
                }
                handle.unconfigure()?;
                Ok(handle)
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

        const HEADER_SIZE: usize = size_of::<Header>();
        const MIN_URB_SUBMIT_SIZE: usize =
            size_of::<UrbHeader>() + size_of::<TransferHeader>() + size_of::<IsoPacketHeader>();

        trace!("starting lender loop");
        'outer: loop {
            if buf_rx.fill_until(&mut rx, HEADER_SIZE).await?.is_none() {
                break 'outer;
            }

            let mut header: Header = buf_rx.read().unwrap();

            // Parse request big-time
            match header.command {
                msg::Command::CmdSubmit => {
                    trace!("got urb submit");

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
                                min_len = buf_rx.len() + num_bytes_needed;
                            }
                        }
                    };

                    let urb = dbg!(&mut frame.header);
                    let (transfer_header, rest) =
                        TransferHeader::mut_from_prefix(&mut frame.data).unwrap();
                    let (transfer, rest) = <[u8]>::mut_from_prefix_with_elems(
                        rest,
                        usize::from(transfer_header.aligned_len),
                    )
                    .unwrap();
                    let iso_packets = IsoPacketData::try_mut_from_bytes(rest).unwrap();
                    let ctrl = urb.setup_packet;

                    let status = match (urb.kind, urb.endpoint.direction()) {
                        (UrbType::Ctrl, Dir::In) => {
                            match handle.read_control(
                                ctrl.bm_request_type,
                                ctrl.b_request as u8,
                                ctrl.w_value,
                                ctrl.w_index,
                                &mut transfer[..usize::from(transfer_header.actual_len)],
                                Duration::from_millis(600),
                            ) {
                                Ok(bytes_written) => {
                                    transfer_header.actual_len = bytes_written as u16;
                                    transfer_header.aligned_len = bytes_written
                                        .next_multiple_of(8)
                                        .try_into()
                                        .unwrap_or(transfer_header.aligned_len);
                                    vhci::Status::Success
                                }
                                Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
                                Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
                                Err(rusb::Error::Io) => vhci::Status::Error,
                                Err(err) => {
                                    warn! { %err, "couldn't read from ctrl" };
                                    vhci::Status::Stall
                                }
                            }
                        }
                        (UrbType::Ctrl, Dir::Out)
                            if STANDARD_DEVICE_SET_CONFIGURATION
                                == (ctrl.request_type(), ctrl.req()) =>
                        {
                            let desired = (ctrl.value() & 0xFF) as u8;
                            if handle
                                .device()
                                .active_config_descriptor()
                                .is_ok_and(|config| desired == config.number())
                            {
                                vhci::Status::Success
                            } else {
                                match handle.set_active_configuration(desired) {
                                    Ok(_) => vhci::Status::Success,
                                    Err(err) => {
                                        warn! { %err, "couldn't set configuration" };
                                        vhci::Status::Stall
                                    }
                                }
                            }
                        }
                        (UrbType::Ctrl, Dir::Out) => {
                            if Req::GetInterface == ctrl.req() {
                                let interface = (ctrl.index() & 0xFF) as u8;
                                let _ = handle.detach_kernel_driver(interface);
                                handle.claim_interface(interface).unwrap();
                                trace!("successfully claimed interface {interface}");
                                let alternate = transfer.get(0).copied().unwrap_or_default();
                                if interface != alternate {
                                    let _ = handle.detach_kernel_driver(alternate);
                                    handle.claim_interface(alternate).unwrap();
                                    trace!("successfully claimed interface {alternate}");
                                }
                            }
                            match handle.write_control(
                                ctrl.bm_request_type,
                                ctrl.b_request as u8,
                                ctrl.w_value,
                                ctrl.w_index,
                                &mut transfer[..usize::from(transfer_header.actual_len)],
                                Duration::from_millis(600),
                            ) {
                                Ok(_) => vhci::Status::Success,
                                Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
                                Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
                                Err(rusb::Error::Io) => vhci::Status::Error,
                                Err(err) => {
                                    warn! { %err, "couldn't write to control" };
                                    vhci::Status::Stall
                                }
                            }
                        }
                        (UrbType::Iso, _) => vhci::Status::Stall,
                        (UrbType::Int, Dir::In) => {
                            match handle.read_interrupt(
                                urb.endpoint.0,
                                &mut transfer[..usize::from(transfer_header.actual_len)],
                                // Duration::from_millis(urb.interval.into()),
                                Duration::from_millis(5500),
                            ) {
                                Ok(bytes_written) => {
                                    transfer_header.actual_len = bytes_written as u16;
                                    transfer_header.aligned_len = bytes_written
                                        .next_multiple_of(8)
                                        .try_into()
                                        .unwrap_or(transfer_header.aligned_len);
                                    vhci::Status::Success
                                }
                                Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
                                Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
                                Err(rusb::Error::Overflow) => vhci::Status::BufferOverrun,
                                Err(rusb::Error::Io) => vhci::Status::Error,
                                Err(err) => {
                                    warn! { %err, "couldn't read interrupt" };
                                    vhci::Status::Stall
                                }
                            }
                        }
                        (UrbType::Int, Dir::Out) => todo!(),
                        (UrbType::Bulk, Dir::In) => todo!(),
                        (UrbType::Bulk, Dir::Out) => todo!(),
                    };

                    urb.status = status;

                    header.command = msg::Command::RetSubmit;
                    header.status = match status {
                        vhci::Status::Pending => todo!(),
                        vhci::Status::Error => msg::Status::DevErr,
                        // vhci::Status::DeviceDisabled => {
                        //     todo!()
                        // }
                        vhci::Status::DeviceDisconnected => msg::Status::NoDev,
                        vhci::Status::BitStuff => todo!(),
                        vhci::Status::Crc => todo!(),
                        vhci::Status::NoResponse => todo!(),
                        vhci::Status::Babble => todo!(),
                        vhci::Status::BufferUnderrun => todo!(),
                        vhci::Status::AllIsoPacketsFailed => todo!(),
                        vhci::Status::ShortPacket => todo!(),
                        vhci::Status::Canceled => todo!(),
                        vhci::Status::Success | _ => msg::Status::Success,
                    };

                    if msg::Status::Success == header.status {
                        let mut response = header
                            .as_bytes()
                            .chain(urb.as_bytes())
                            .chain(transfer_header.as_bytes())
                            .chain(&transfer[..usize::from(transfer_header.aligned_len)])
                            .chain(iso_packets.as_buf());
                        tx.write_all_buf(&mut response).await?;
                    } else {
                        let _errno = dbg!(io::Error::last_os_error());
                        let mut response = header.as_bytes();
                        tx.write_all_buf(&mut response).await?;
                        tx.close().await?;
                        return Err(Error::ReqFailed);
                    }

                    let size_of_frame = size_of_val(frame);
                    buf_rx.consume(size_of_frame);
                }
                msg::Command::CmdUnlink => {
                    trace!("got urb unlink");

                    // TODO: If we ever figure out asynchronous transfer,
                    //       then implement this for reals

                    header.command = msg::Command::RetUnlink;
                    header.status = msg::Status::Success;
                    let mut response = header.as_bytes();
                    tx.write_all_buf(&mut response).await?;
                }
                msg::Command::CmdPort => {
                    trace!("got port reset");

                    header.status = if header.reset {
                        match handle.reset() {
                            Ok(_) => msg::Status::Success,
                            Err(err) => {
                                error! { %err, "error while unconfiguring device {dev_id:?}" };
                                msg::Status::DevErr
                            }
                        }
                    } else {
                        msg::Status::Success
                    };

                    header.command = msg::Command::RetPort;
                    header.reset = false;

                    trace!("port has been reset");

                    // let mut response = dbg!(&header).as_bytes();
                    let mut response = header.as_bytes();
                    tx.write_all_buf(&mut response).await?;
                    if msg::Status::Success != header.status {
                        tx.close().await?;
                        return Err(Error::ReqFailed);
                    }
                }
                _ => unreachable!("smh smh client"),
            }
        }
        todo!()
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

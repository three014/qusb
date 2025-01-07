use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use proto::{
    data::{Data, ReadError, Ring},
    msg::{
        self, Header, IsoPacketData, IsoPacketGiveback, IsoPacketHeader, Transfer, TransferHeader,
        UrbHeader,
    },
};
use rusb::UsbContext;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace};
use vhci::{ioctl, usbfs::STANDARD_DEVICE_SET_ADDRESS, DataRate, PortChange, PortFlag, PortStatus};
use zerocopy::{IntoBytes, TryFromBytes};

use crate::{
    iso,
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
    iso_tx: iso::Sender,
    iso_rx: iso::Receiver,
    vhci: stub::Controller,
    id: msg::UsbDeviceId,
}

impl<W, R> ClientBorrowDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        iso_tx: iso::Sender,
        iso_rx: iso::Receiver,
        vhci: stub::Controller,
        id: msg::UsbDeviceId,
    ) -> Self {
        Self {
            tx,
            rx,
            buf_rx,
            iso_tx,
            iso_rx,
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
            iso_tx,
            iso_rx,
            mut vhci,
            id: dev_id,
        } = self;
        let mut buf_tx = BytesMut::with_capacity(1024);

        // Step 1: Connect VHCI port
        let (port, mut work_rx) = vhci
            .register(RegisterPort::Any, DataRate::High)
            .await
            .unwrap();

        enum Event {
            Work(Option<ioctl::Work>),
            IsoUrbResp(Option<Bytes>),
            UrbResp(io::Result<usize>),
        }

        // Step 2: Setup context
        const HEADER_SIZE: usize = size_of::<Header>();
        const MIN_URB_SUBMIT_SIZE: usize =
            size_of::<UrbHeader>() + size_of::<TransferHeader>() + size_of::<IsoPacketHeader>();

        let seqnum = AtomicU64::new(0);
        let mut addr: u8 = 0xff;
        let mut prev = ioctl::IocPortStat::default();
        let mut seqnums =
            SimpleMap::<u64, ioctl::UrbHandle>::with_capacity_and_hasher(32, Default::default());
        let mut handles =
            SimpleMap::<ioctl::UrbHandle, u64>::with_capacity_and_hasher(32, Default::default());

        trace!("starting event loop");

        // Step 3: Start event loop
        loop {
            let event = tokio::select! {
                maybe_work = work_rx.recv() => {
                    Event::Work(maybe_work)
                }
                maybe_bytes = buf_rx.fill_with_reader(&mut rx) => {
                    Event::UrbResp(maybe_bytes)
                }
            };

            debug!("==============================================");
            match event {
                Event::Work(Some(ioctl::Work::PortStat(next))) => {
                    debug!("got port stat for {:?}", next.index());
                    debug!("status: {:?}", next.status());
                    debug!("change: {:?}", next.change());
                    debug!("index: {:?}", next.index());
                    debug!("flags: {:?}", next.flags());
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
                        trace!("port is resetting -> completing reset");
                        vhci.reset_done(next.index(), true).unwrap();
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
                    if ioctl::UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && STANDARD_DEVICE_SET_ADDRESS
                            == (urb.setup_packet.request_type(), urb.setup_packet.req()) =>
                {
                    addr = urb.setup_packet.value().try_into().unwrap();
                    trace!("set address to {addr:#x}");

                    let mut urb = vhci::UrbWithData::from_ioctl(urb, handle);
                    urb.set_status(vhci::Status::Success);

                    vhci.giveback_urb(urb).await.unwrap();
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle)))) => {
                    assert_eq!(addr, urb.address.get());
                    trace!("got urb for {addr:#x}");
                    let now = Instant::now();
                    let next = seqnum.fetch_add(1, Ordering::Relaxed);
                    seqnums.insert(next, handle);
                    handles.insert(handle, next);

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

                    if vhci::usbfs::Dir::Out == urb.endpoint.direction()
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

                    // And away we go!!!!
                    let dur = now.elapsed();
                    if Duration::from_micros(15) < dur {
                        trace!("took {:?} to setup URB frame for sending", dur);
                    }

                    trace!("about to write {} bytes to buffer", buf_tx.len());
                    tx.write_all_buf(&mut &buf_tx[..reserve_size])
                        .await
                        .unwrap();
                }
                Event::Work(Some(ioctl::Work::CancelUrb(handle))) => {
                    // FIXME: Removing this before getting back the URB
                    //        results in a panic!().
                    //        If handle or seqnum is missing, then
                    //        we disregard the URB because it was cancelled.
                    if let Some(seqnum) = handles.remove(&handle) {
                        let _ = seqnums.remove(&seqnum);
                    }

                    let next = seqnum.fetch_add(1, Ordering::Relaxed);
                    seqnums.insert(next, handle);
                    handles.insert(handle, next);

                    // Calculate the total size of the URB frame...
                    let reserve_size = size_of::<Header>() + size_of::<ioctl::UrbHandle>();

                    // ...and make sure we have enough space in our scratch buffer
                    buf_tx.clear();
                    buf_tx.reserve(reserve_size.saturating_sub(buf_tx.capacity()));
                    buf_tx.put_bytes(0, reserve_size);

                    // By this point `reserved_chunk` should be a zero-initialized
                    // slice of u8's ready for writing our data
                    let reserved_chunk = &mut buf_tx[..reserve_size];

                    // Grab mutable references for each part of our frame
                    let (header, rest) = Header::try_mut_from_prefix(reserved_chunk).unwrap();
                    let urb_handle = ioctl::UrbHandle::try_mut_from_bytes(rest).unwrap();

                    *header = Header {
                        seqnum: next,
                        dev_id,
                        command: msg::Command::CmdUnlink,
                        status: msg::Status::Success,
                        _padding: Default::default(),
                    };
                    *urb_handle = handle;

                    trace!("about to write {} bytes into the buffer", buf_tx.len());
                    tx.write_all_buf(&mut buf_tx).await.unwrap();
                }
                Event::IsoUrbResp(_bytes) => {
                    todo!("will eventually be handled almost the same way as normal urbs")
                }
                Event::UrbResp(Ok(_)) if HEADER_SIZE < buf_rx.len() => {
                    let now = Instant::now();
                    let header: Header = buf_rx.read().unwrap();
                    let handle = seqnums.remove(&header.seqnum).unwrap();
                    let _ = handles.remove(&handle);
                    match header.status {
                        msg::Status::Success => {
                            while MIN_URB_SUBMIT_SIZE > buf_rx.len() {
                                if 0 == buf_rx.fill_with_reader(&mut rx).await? {
                                    break;
                                }
                            }
                            let urb: UrbHeader = buf_rx.read().unwrap();
                            let mut transfer: Data<Transfer> = buf_rx.claim_dst().unwrap();
                            let mut iso_packets: Data<IsoPacketGiveback> =
                                buf_rx.claim_dst().unwrap();

                            let lender_urb = UrbWithIsoGiveback {
                                handle,
                                header: &urb,
                                transfer: &mut transfer.get_mut().get_mut(),
                                iso_giveback: &mut iso_packets.get_mut().get_mut(),
                            };

                            let dur = now.elapsed();
                            if Duration::from_micros(15) < dur {
                                trace!("took {:?} to unpack URB frame for giveback", dur);
                            }
                            vhci.giveback_urb(lender_urb).await.unwrap();
                        }
                        msg::Status::Failed => todo!(),
                        msg::Status::DevBusy => todo!(),
                        msg::Status::DevErr => todo!(),
                        msg::Status::NoDev => todo!(),
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
    iso_tx: iso::Sender,
    iso_rx: iso::Receiver,
    id: msg::UsbDeviceId,
}

impl<W, R> LendDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        iso_tx: iso::Sender,
        iso_rx: iso::Receiver,
        id: msg::UsbDeviceId,
    ) -> Self {
        Self {
            tx,
            rx,
            buf_rx,
            iso_tx,
            iso_rx,
            id,
        }
    }
}

impl<W, R> LendDevice<W, R>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
    R: AsyncRead + Unpin,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn lend(self) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            iso_tx,
            iso_rx,
            id: dev_id,
        } = self;

        let (languages, handle, dev) = match rusb::Context::new()
            .and_then(|ctx| ctx.devices())
            .and_then(|list| {
                list.iter()
                    .find(|dev| {
                        dev_id.bus_number == dev.bus_number() && dev_id.device_addr == dev.address()
                    })
                    .ok_or(rusb::Error::NoDevice)
            })
            .and_then(|dev| Ok((dev.open()?, dev)))
            .and_then(|(handle, dev)| {
                Ok((
                    handle.read_languages(Duration::from_millis(500))?,
                    handle,
                    dev,
                ))
            }) {
            Ok(dev) => {
                let mut response = msg::Status::Success.as_bytes().chain(&[0u8; 7][..]);
                tx.write_all_buf(&mut response).await?;
                dev
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
        const URB_UNLINK_SIZE: usize = size_of::<vhci::ioctl::UrbHandle>();
        const MIN_URB_SUBMIT_SIZE: usize =
            size_of::<UrbHeader>() + size_of::<TransferHeader>() + size_of::<IsoPacketHeader>();

        trace!("starting lender loop");
        'outer: loop {
            while HEADER_SIZE > buf_rx.len() {
                if 0 == buf_rx.fill_with_reader(&mut rx).await? {
                    break 'outer;
                }
            }

            let mut header: Header = buf_rx.read().unwrap();

            // Parse request big-time
            match header.command {
                msg::Command::CmdSubmit => {
                    trace!("got urb submit");

                    while MIN_URB_SUBMIT_SIZE > buf_rx.len() {
                        if 0 == buf_rx.fill_with_reader(&mut rx).await? {
                            break 'outer;
                        }
                    }

                    let mut urb: UrbHeader = buf_rx.read().unwrap();
                    let mut transfer: Data<Transfer> = match buf_rx.claim_dst() {
                        Ok(transfer) => transfer,
                        Err(ReadError::BufferShort) => {
                            if 0 == buf_rx.fill_with_reader(&mut rx).await? {
                                break 'outer;
                            }

                            buf_rx.claim_dst().unwrap()
                        }
                        Err(ReadError::CorruptedData) => todo!(),
                    };
                    let mut iso_packets: Data<IsoPacketData> = match buf_rx.claim_dst() {
                        Ok(transfer) => transfer,
                        Err(ReadError::BufferShort) => {
                            if 0 == buf_rx.fill_with_reader(&mut rx).await? {
                                break 'outer;
                            }

                            buf_rx.claim_dst().unwrap()
                        }
                        Err(ReadError::CorruptedData) => todo!(),
                    };

                    let status = match (urb.kind, urb.endpoint.direction()) {
                        (ioctl::UrbType::Ctrl, vhci::usbfs::Dir::In) => {
                            let transfer_ref = transfer.get_mut();
                            let ctrl = urb.setup_packet;
                            match handle.read_control(
                                ctrl.bm_request_type,
                                ctrl.b_request as u8,
                                ctrl.w_value,
                                ctrl.w_index,
                                transfer_ref.get_mut(),
                                Duration::from_millis(300),
                            ) {
                                Ok(bytes_written) => {
                                    transfer_ref.header.actual_len = bytes_written as u16;
                                    transfer_ref.header.aligned_len = bytes_written
                                        .next_multiple_of(8)
                                        .try_into()
                                        .unwrap_or(transfer_ref.header.aligned_len);
                                    vhci::Status::Success
                                }
                                Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
                                Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
                                Err(_) => vhci::Status::Stall,
                            }
                        }
                        (ioctl::UrbType::Ctrl, vhci::usbfs::Dir::Out) => {
                            let ctrl = urb.setup_packet;
                            match handle.write_control(
                                ctrl.bm_request_type,
                                ctrl.b_request as u8,
                                ctrl.w_value,
                                ctrl.w_index,
                                transfer.get_mut().get_mut(),
                                Duration::from_millis(300),
                            ) {
                                Ok(_) => vhci::Status::Success,
                                Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
                                Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
                                Err(_) => vhci::Status::Stall,
                            }
                        }
                        (ioctl::UrbType::Iso, _) => vhci::Status::Stall,
                        (ioctl::UrbType::Int, vhci::usbfs::Dir::In) => {
                            let transfer_ref = transfer.get_mut();
                            match handle.read_interrupt(
                                urb.endpoint.0,
                                transfer_ref.get_mut(),
                                Duration::from_millis(50),
                            ) {
                                Ok(bytes_written) => {
                                    transfer_ref.header.actual_len = bytes_written as u16;
                                    transfer_ref.header.aligned_len = bytes_written
                                        .next_multiple_of(8)
                                        .try_into()
                                        .unwrap_or(transfer_ref.header.aligned_len);
                                    vhci::Status::Success
                                }
                                Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
                                Err(rusb::Error::Timeout) => vhci::Status::TimedOut,
                                Err(rusb::Error::Overflow) => vhci::Status::BufferOverrun,
                                Err(_) => vhci::Status::Stall,
                            }
                        }
                        (ioctl::UrbType::Int, vhci::usbfs::Dir::Out) => todo!(),
                        (ioctl::UrbType::Bulk, vhci::usbfs::Dir::In) => todo!(),
                        (ioctl::UrbType::Bulk, vhci::usbfs::Dir::Out) => todo!(),
                    };

                    urb.status = status;

                    header.command = msg::Command::RetSubmit;
                    header.status = match status {
                        vhci::Status::Pending => todo!(),
                        vhci::Status::Error => todo!(),
                        vhci::Status::TimedOut => todo!(),
                        vhci::Status::DeviceDisabled => todo!(),
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
                            .chain(transfer.as_bytes())
                            .chain(iso_packets.as_bytes());
                        tx.write_all_buf(&mut response).await?;
                    } else {
                        let mut response = header.as_bytes();
                        tx.write_all_buf(&mut response).await?;
                        tx.close().await?;
                        return Err(Error::Unknown);
                    }
                }
                msg::Command::CmdUnlink => {
                    trace!("got urb unlink");
                    while URB_UNLINK_SIZE > buf_rx.len() {
                        if 0 == buf_rx.fill_with_reader(&mut rx).await? {
                            break 'outer;
                        }
                    }

                    todo!("read handle and cancel request");
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

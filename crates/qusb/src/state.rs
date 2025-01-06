use std::{
    io,
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use proto::{
    data::{Data, Ring},
    msg::{self, Header, IsoPacketData, IsoPacketGiveback, Transfer, UrbHeader},
};
use rusb::{UsbContext, UsbOption};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace, Instrument};
use vhci::{
    ioctl,
    usbfs::{
        DescriptorType, STANDARD_DEVICE_GET_CONFIGURATION, STANDARD_DEVICE_GET_DESCRIPTOR,
        STANDARD_DEVICE_SET_ADDRESS,
    },
    DataRate, PortChange, PortFlag, PortStatus,
};
use zerocopy::{IntoBytes, TryFromBytes};

use crate::{
    dev::{self, RegisterPort},
    iso,
    utils::{self, SimpleMap},
    Error, RusbError, UrbWithIsoData, UrbWithIsoGiveback,
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
    vhci: dev::Controller,
    id: msg::UsbDeviceId,
}

impl<W, R> ClientBorrowDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        iso_tx: iso::Sender,
        iso_rx: iso::Receiver,
        vhci: dev::Controller,
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
    pub async fn run(self) {
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
        let seqnum = AtomicU64::new(0);
        let mut addr: u8 = 0xff;
        let mut prev = ioctl::IocPortStat::default();
        let mut seqnums =
            SimpleMap::<u64, ioctl::UrbHandle>::with_capacity_and_hasher(32, Default::default());
        let mut handles =
            SimpleMap::<ioctl::UrbHandle, u64>::with_capacity_and_hasher(32, Default::default());

        // Step 3: Start event loop
        loop {
            let event = tokio::select! {
                maybe_work = work_rx.recv() => {
                    Event::Work(maybe_work)
                }
                maybe_bytes = buf_rx.read_into_from_reader(&mut rx) => {
                    Event::UrbResp(maybe_bytes)
                }
            };

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
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, _))))
                    if ioctl::UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && STANDARD_DEVICE_SET_ADDRESS
                            == (urb.setup_packet.request_type(), urb.setup_packet.req()) =>
                {
                    addr = urb.setup_packet.w_value.try_into().unwrap();
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle)))) => {
                    assert_eq!(addr, urb.address.get());
                    let now = Instant::now();
                    let next = seqnum.fetch_add(1, Ordering::Relaxed);
                    seqnums.insert(next, handle);
                    handles.insert(handle, next);

                    // TODO: Do we reclaim the reserved chunk automatically??
                    //       - Seems like we gotta manually give back the chunk
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
                    let transfer_padded_size: usize =
                        NonZeroUsize::new(usize::try_from(urb.buffer_length).unwrap())
                            .map(|len| len.get().next_multiple_of(8))
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

                    if urb.buffer_length > 0 || urb.packet_count > 0 {
                        let borrower_urb = UrbWithIsoData {
                            handle,
                            header: urb_header,
                            transfer: &mut transfer.buf[..transfer.header.actual_len as usize],
                            iso_data: &mut iso.buf[..iso.header.len as usize],
                        };

                        vhci.fetch_data(borrower_urb).unwrap();
                    }

                    // And away we go!!!!
                    let dur = now.elapsed();
                    if Duration::from_micros(15) < dur {
                        trace!("Took {:?} to setup URB frame for sending", now);
                    }
                    tx.write_all_buf(&mut buf_tx).await.unwrap();
                }
                Event::Work(Some(ioctl::Work::CancelUrb(handle))) => {
                    if let Some(seqnum) = handles.remove(&handle) {
                        let _ = seqnums.remove(&seqnum);
                    }

                    let next = seqnum.fetch_add(1, Ordering::Relaxed);
                    seqnums.insert(next, handle);

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
                            let urb: UrbHeader = buf_rx.read().unwrap();
                            let mut transfer: Data<Transfer> = buf_rx.claim_dst().unwrap();
                            let transfer_ref = transfer.get_mut();
                            let mut iso_packets: Data<IsoPacketGiveback> =
                                buf_rx.claim_dst().unwrap();
                            let iso_packets_ref = iso_packets.get_mut();

                            let lender_urb = UrbWithIsoGiveback {
                                handle,
                                header: &urb,
                                transfer: &mut transfer_ref.buf
                                    [..transfer_ref.header.actual_len as usize],
                                iso_giveback: &mut iso_packets_ref.buf
                                    [..iso_packets_ref.header.len as usize],
                            };

                            vhci.giveback_urb(lender_urb).unwrap();
                            let dur = now.elapsed();
                            if Duration::from_micros(15) < dur {
                                trace!("Took {:?} to unpack URB frame for giveback", now);
                            }
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
                Event::UrbResp(Err(err)) => todo!(),
                Event::UrbResp(Ok(0)) => break,
                Event::Work(None) => todo!("how did we get here? should we shutdown here?"),
                _ => (),
            }
        }
    }
}

pub enum ServerResp<W, R> {
    ListDevices(ServerListDevices<W>),
    BorrowDevice(ServerLendDevice<W, R>),
}

pub struct ServerLendDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    iso_tx: iso::Sender,
    iso_rx: iso::Receiver,
    id: msg::UsbDeviceId,
}

impl<W, R> ServerLendDevice<W, R> {
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

impl<W, R> ServerLendDevice<W, R>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
    R: AsyncRead + Unpin,
{
    pub async fn run(self) -> Result<(), Error> {
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
            Ok(dev) => dev,
            Err(err) => {
                let err = RusbError { kind: err, dev_id };
                let ret_err = Error::from(err);
                let status = msg::Status::from(err);

                tx.write_all_buf(&mut status.as_bytes()).await.unwrap();
                tx.write_all_buf(&mut &[0, 0, 0, 0, 0, 0, 0][..])
                    .await
                    .unwrap();
                tx.close().await.unwrap();
                return Err(ret_err);
            }
        };

        const HEADER_SIZE: usize = size_of::<Header>();

        loop {
            match buf_rx.read_into_from_reader(&mut rx).await {
                Ok(_) if HEADER_SIZE < buf_rx.len() => {
                    let header: Header = buf_rx.read().unwrap();

                    // Parse request big-time
                    match header.command {
                        msg::Command::CmdSubmit => {
                            let mut urb: UrbHeader = buf_rx.read().unwrap();
                            let mut transfer: Data<Transfer> = buf_rx.claim_dst().unwrap();
                            let mut iso_packets: Data<IsoPacketData> = buf_rx.claim_dst().unwrap();
                            match urb.kind {
                                ioctl::UrbType::Ctrl => {
                                    let ctrl = urb.setup_packet;
                                    match (ctrl.request_type(), ctrl.req()) {
                                        STANDARD_DEVICE_GET_DESCRIPTOR => {
                                            let index = u8::try_from(ctrl.value() & 0xff).unwrap();
                                            // IDEA: Manually, painstakingly fill in transfer buffer
                                            //       with each piece of data. Ow.
                                            match  DescriptorType::from_u8(
                                                u8::try_from(ctrl.value() >> 8).unwrap(),
                                            ) {
                                                Some(DescriptorType::Device) => {
                                                    let desc = dev.device_descriptor().unwrap();
                                                    let transfer_ref = transfer.get_mut();
                                                    let (len, short) = if u16::from(desc.length()) < transfer_ref.header.actual_len {
                                                        (u16::from(desc.length()), true)
                                                    } else {
                                                        (transfer_ref.header.actual_len, false)
                                                    };

                                                    urb.status = if short { vhci::Status::ShortPacket } else { vhci::Status::Success };
                                                }
                                                Some(DescriptorType::Configuration) => {
                                                    let desc =
                                                        dev.config_descriptor(index).unwrap();
                                                }
                                                Some(DescriptorType::String) => {
                                                    // Needs open device
                                                    let lang = languages.iter().find(|lang| ctrl.index() == lang.lang_id()).copied();
                                                    let desc = handle.read_string_descriptor(lang.unwrap_or(languages[0]), index, Duration::from_millis(500)).unwrap();
                                                }
                                                Some(DescriptorType::Interface) => todo!(),
                                                Some(DescriptorType::Endpoint) => todo!(),
                                                None => todo!("thinking of mapping each unknown value to a connection break >:)"),
                                            }
                                        }
                                        STANDARD_DEVICE_GET_CONFIGURATION => {
                                            let transfer_ref = transfer.get_mut();
                                            let conf = handle.active_configuration().unwrap();
                                            transfer_ref.buf[0] = conf;
                                            transfer_ref.header.actual_len = 1;
                                        }
                                        _ => todo!("implement all ctrl requests"),
                                    }
                                }
                                ioctl::UrbType::Iso => todo!(),
                                ioctl::UrbType::Int => todo!(),
                                ioctl::UrbType::Bulk => todo!(),
                            }
                        }
                        msg::Command::CmdUnlink => todo!(),
                        _ => unreachable!("smh smh client"),
                    }
                }
                Ok(0) => break,
                Err(err) => todo!(),
                _ => (),
            }
        }
        todo!()
    }
}

pub struct ServerListDevices<W> {
    tx: W,
}

impl<W> ServerListDevices<W> {
    pub fn new(tx: W) -> Self {
        Self { tx }
    }
}

impl<W> ServerListDevices<W>
where
    W: AsyncWrite + utils::CloseStream + Unpin,
{
    pub async fn resp_list_devices<'a, I, T>(
        self,
        iter: impl Fn() -> io::Result<I>,
    ) -> Result<(), Error>
    where
        I: Iterator<Item = T>,
        T: msg::SendUsbDeviceInfo,
    {
        use tokio::io::AsyncWriteExt;
        let mut tx = self.tx;

        let devices = match iter() {
            Ok(devices) => {
                tx.write_all(msg::Status::Success.as_bytes()).await?;
                tx.write_all(&[0, 0, 0]).await?;
                devices
            }
            Err(err) => {
                tx.write_all(msg::Status::Failed.as_bytes()).await?;
                return Err(err.into());
            }
        };

        for usb in devices {
            let main_info = usb.get();
            tx.write_all(main_info.as_bytes()).await?;
            tx.write_all(usb.interfaces_with_padding().as_bytes())
                .await?;
        }

        tx.close().await.map_err(Error::from)
    }
}

use std::{
    collections::{BTreeMap, VecDeque},
    io,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use bytes::{Buf, BytesMut};
use nohash_hasher::{IntMap, IntSet};
use proto::{
    data::{Data, ReadError, Ring},
    msg::{self, Header, QusbFrame, UrbFrame, UrbHeader},
};
use rusb::UsbContext;
use rusb_async::{
    DeviceHandleExt, InnerTransfer, IsoPacket, TransferFlags, TransferStatus, UsbMemMut,
};
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
    utils::{self, align_to_usize, SimpleMap, Timer},
    Error, Result, RusbError, UrbWithIsoData, UrbWithIsoGiveback,
};

/// Rust-representation of a peer request
pub enum ClientReq {
    ListDevices,
    BorrowDevice(msg::UsbDeviceId),
    LendDevice(msg::UsbDeviceId),
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

#[tracing::instrument(level = "trace", skip_all)]
async fn handle_port_stat<W: AsyncWrite + Unpin>(
    next: ioctl::IocPortStat,
    prev: ioctl::IocPortStat,
    addr: &mut u8,
    seqnum: &AtomicU32,
    seqnums: &mut SimpleMap<ioctl::UrbHandle, u32>,
    handles: &mut SimpleMap<u32, ioctl::UrbHandle>,
    mut tx: W,
) {
    let timer = Timer::start();
    let status = next.status();
    let change = next.change();
    let flags = next.flags();
    if change.contains(PortChange::CONNECTION) {
        debug!("CONNECTION state changed -> invalidating address");
        *addr = 0xff;
    } else if change.contains(PortChange::RESET)
        && (!status).contains(PortStatus::RESET)
        && status.contains(PortStatus::ENABLE)
    {
        debug!("RESET successful -> use default address");
        *addr = 0;
    } else if prev.status().contains(PortStatus::POWER) && (!status).contains(PortStatus::POWER) {
        debug!("port is powered off");
    } else if (!prev.status()).contains(PortStatus::RESET)
        && status.contains(PortStatus::RESET | PortStatus::CONNECTION)
    {
        let next_seqnum = seqnum.fetch_add(1, Ordering::Relaxed);
        // We pray that we don't run into another handle
        let handle = ioctl::UrbHandle(rand::random());
        seqnums.insert(handle, next_seqnum);
        handles.insert(next_seqnum, handle);
        debug!("({next_seqnum}) port is resetting");

        let header = Header {
            total_frame_len: (size_of::<Header>() / 8) as u16,
            seqnum: next_seqnum,
            command: msg::Command::CmdPort,
            status: msg::Status::Success,
        };

        timer.stop_and_report(
            Some(Duration::from_nanos(100)),
            "setting up port reset frame",
        );
        let mut request = header.as_bytes();
        tx.write_all_buf(&mut request).await.unwrap();
    } else if (!prev.flags()).contains(PortFlag::RESUMING)
        && flags.contains(PortFlag::RESUMING)
        && status.contains(PortStatus::CONNECTION)
    {
        debug!("port is resuming -> completing resume");
    } else {
        debug!("status: {:?}", next.status());
        debug!("change: {:?}", next.change());
        debug!("index: {:?}", next.index());
        debug!("flags: {:?}", next.flags());
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
struct BorrowId {
    remote_dev: msg::UsbDeviceId,
    local_port: vhci::Port,
}

impl std::fmt::Debug for BorrowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Id")
            .field("remove_dev", &self.remote_dev)
            .field("local_port", &self.local_port.get())
            .finish()
    }
}

impl std::fmt::Display for BorrowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Remote Dev {:03}/{:03}, Local Port {}",
            self.remote_dev.bus_number,
            self.remote_dev.device_addr,
            self.local_port.get()
        )
    }
}

/// A struct containing the logic for
/// borrowing a USB device from a lender.
pub struct BorrowDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    vhci: stub::Controller,
    id: msg::UsbDeviceId,
}

impl<W, R> BorrowDevice<W, R> {
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

impl<W, R> BorrowDevice<W, R>
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

        let id = BorrowId {
            remote_dev: _dev_id,
            local_port: port,
        };

        enum Event {
            Work(Option<ioctl::Work>),
            Frame(io::Result<Option<Recv>>),
        }

        let seqnum = AtomicU32::new(0);
        let mut addr: u8 = 0xff;
        let mut prev = ioctl::IocPortStat::default();
        let mut handles: SimpleMap<u32, ioctl::UrbHandle> =
            SimpleMap::with_capacity_and_hasher(32, Default::default());
        let mut seqnums: SimpleMap<ioctl::UrbHandle, u32> =
            SimpleMap::with_capacity_and_hasher(32, Default::default());

        info!("({id}) starting event loop");

        let result: Result<()> = loop {
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
                    debug!("({id}) got port stat");
                    handle_port_stat(
                        next,
                        prev,
                        &mut addr,
                        &seqnum,
                        &mut seqnums,
                        &mut handles,
                        &mut tx,
                    )
                    .await;
                    prev = next;
                }
                Event::Work(Some(ioctl::Work::ProcessUrb((urb, handle))))
                    if UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
                {
                    addr = urb.setup_packet.value() as u8;
                    debug!("({id}) set local dev address to {addr:03}");

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
                    debug!("({id}) got urb (DevAddr({addr:03}))");
                    let timer = Timer::start();

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

                            timer.stop_and_report(
                                Some(Duration::from_nanos(500)),
                                "setting up URB frame for sending",
                            );
                            tx.write_all_buf(&mut request).await.unwrap();
                        }
                        Dir::Out | Dir::In => {
                            let mut request = header.as_bytes().chain(urb_header.as_bytes());
                            timer.stop_and_report(
                                Some(Duration::from_nanos(50)),
                                "setting up URB frame for sending",
                            );
                            tx.write_all_buf(&mut request).await.unwrap();
                        }
                    }
                }
                Event::Work(Some(ioctl::Work::CancelUrb(handle))) => {
                    debug!("({id}) got cancel urb ({handle:?})");
                    if let Some(&seqnum) = seqnums.get(&handle) {
                        let header = Header {
                            total_frame_len: (size_of::<Header>() / 8) as u16,
                            seqnum,
                            command: msg::Command::CmdUnlink,
                            status: msg::Status::Success,
                        };

                        let mut request = header.as_bytes();
                        tx.write_all_buf(&mut request).await.unwrap();
                    } else {
                        debug!("{handle:?} had already been returned");
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
                    let timer = Timer::start();
                    let handle = handles.remove(&seqnum).unwrap();
                    let _ = seqnums.remove(&handle).unwrap();
                    let frame = urb.get_mut();
                    let urb = &mut frame.header;

                    if vhci::Status::Success != urb.status {
                        warn!("({id}) transfer {} failed: {:?}", seqnum, urb.status);
                    }
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

                    timer.stop_and_report(
                        Some(Duration::from_nanos(500)),
                        "unpacking URB for giveback",
                    );

                    vhci.giveback_urb(lender_urb).await.unwrap();
                }
                Event::Frame(Ok(Some(Recv::PortReset(Header {
                    seqnum,
                    status: msg::Status::Success,
                    ..
                })))) => {
                    let handle = handles.remove(&seqnum).unwrap();
                    let _ = seqnums.remove(&handle).unwrap();
                    debug!("({id}) port has been reset");
                    vhci.reset_done(port, true).unwrap();
                }
                Event::Frame(Ok(Some(Recv::Urb((Header { status, .. }, _)))))
                | Event::Frame(Ok(Some(Recv::PortReset(Header { status, .. })))) => match status {
                    msg::Status::Failed => todo!(),
                    msg::Status::DevBusy => todo!(),
                    msg::Status::DevErr => break Err(Error::Unknown),
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
    LendDevice(BorrowDevice<W, R>),
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
                    debug!("({seqnum}) claimed interface {interface}");
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
        .and_then(|ctx| ctx.devices())
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
            for interface in 0..16 {
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
            debug!("({seqnum}) port has been reset");
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

#[derive(Debug)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

#[tracing::instrument(level = "trace", skip_all)]
fn get_or_alloc_transfer(
    mut cache: MutexGuard<'_, BTreeMap<u16, OneOrMany<InnerTransfer>>>,
    num_iso_packets: u16,
) -> InnerTransfer {
    let timer = Timer::start();
    let mut maybe_first_entry;
    let maybe_entry = if 0 == num_iso_packets {
        maybe_first_entry = cache.first_entry();
        maybe_first_entry.as_mut().map(|entry| entry.get_mut())
    } else {
        cache
            .range_mut(num_iso_packets as u16..)
            .next()
            .map(|(_k, v)| v)
    };
    if let Some(entry) = maybe_entry {
        const EMPTY: OneOrMany<InnerTransfer> = OneOrMany::Many(Vec::new());
        let (transfer, remove_entry) = {
            match std::mem::replace(entry, EMPTY) {
                OneOrMany::One(transfer) => (transfer, true),
                // Only true if number of packets is zero (Ctrl, Int, or Bulk)
                OneOrMany::Many(vec) if vec.is_empty() => {
                    debug_assert_eq!(num_iso_packets, 0);
                    (InnerTransfer::new(num_iso_packets as usize), false)
                }
                OneOrMany::Many(mut vec) => {
                    let transfer = vec.pop().unwrap();
                    // We only get rid of the vec if the number of packets
                    // wasn't zero (aka an Isochronous transfer).
                    if vec.is_empty() && 0 != num_iso_packets {
                        (transfer, true)
                    } else {
                        *entry = OneOrMany::Many(vec);
                        (transfer, false)
                    }
                }
            }
        };
        if remove_entry {
            cache.remove(&(transfer.num_iso_packets() as u16));
        }
        timer.stop_and_report(Some(Duration::from_nanos(500)), "getting a transfer");
        transfer
    } else {
        drop(cache);
        timer.stop_and_report(Some(Duration::from_nanos(500)), "allocating a transfer");
        InnerTransfer::new(num_iso_packets as usize)
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn insert_spare_transfer(
    mut cache: MutexGuard<'_, BTreeMap<u16, OneOrMany<InnerTransfer>>>,
    transfer: InnerTransfer,
) {
    let timer = Timer::start();
    const EMPTY: OneOrMany<InnerTransfer> = OneOrMany::Many(Vec::new());
    let entry = cache.get_mut(&(transfer.num_iso_packets() as u16));
    match entry {
        Some(entry) => match std::mem::replace(entry, EMPTY) {
            OneOrMany::One(other) => {
                *entry = OneOrMany::Many(vec![transfer, other]);
                timer.stop_and_report(
                    Some(Duration::from_nanos(500)),
                    "inserting a spare transfer into a newly allocated bucket",
                );
            }
            OneOrMany::Many(mut vec) => {
                vec.push(transfer);
                *entry = OneOrMany::Many(vec);
                timer.stop_and_report(
                    Some(Duration::from_nanos(500)),
                    "inserting a spare transfer",
                );
            }
        },
        // Almost all our transfers will be made for Ctrl, Int, or Bulk, so we go ahead
        // and store those transfers in a Vec as a small optimization
        None if 0 == transfer.num_iso_packets() => {
            cache.insert(
                transfer.num_iso_packets() as u16,
                OneOrMany::Many(vec![transfer]),
            );
            timer.stop_and_report(
                Some(Duration::from_nanos(500)),
                "inserting a spare transfer into a newly allocated bucket",
            );
        }
        None => {
            cache.insert(transfer.num_iso_packets() as u16, OneOrMany::One(transfer));
            timer.stop_and_report(
                Some(Duration::from_nanos(500)),
                "inserting a spare transfer",
            );
        }
    }
}

/// An allocation strategy for direct memory access regions.
///
/// The motivation behind this struct is that setting up and tearing down DMA
/// regions on modern systems is slower than compared to normal memory
/// allocation. This is why [`UsbMemMut`] does not allocate more memory
/// when it doesn't have the space to fit more data.
///
/// To get around this, we reserve a cluster of small DMA regions upfront,
/// then handle reservations by trying to reclaim enough space for the requested
/// memory block in each region until one succeeds.
///
/// Is it fast? For the purpose of this project, yeah!
///
/// [`UsbMemMut`]: rusb_async::UsbMemMut
struct DmaAllocator<C: rusb::UsbContext> {
    queue: VecDeque<UsbMemMut>,
    handle: Arc<rusb::DeviceHandle<C>>,
}

const DMA_LEN: usize = 16 << 12;

impl<C: rusb::UsbContext> DmaAllocator<C> {
    pub fn with_capacity(capacity: usize, handle: Arc<rusb::DeviceHandle<C>>) -> Self {
        let mut queue = VecDeque::with_capacity(capacity);
        queue.resize_with(capacity, || unsafe { handle.new_usb_mem(DMA_LEN).unwrap() });

        Self { queue, handle }
    }

    // #[tracing::instrument(level = "trace", skip_all)]
    pub fn reserve(&mut self, num_bytes: usize) -> UsbMemMut {
        let timer = Timer::start();
        assert!(num_bytes <= DMA_LEN, "requested more bytes than can be held in a single block: {num_bytes} bytes (max: {DMA_LEN} bytes)");
        let queue = &mut self.queue;
        for _ in 0..queue.len() {
            let dma = queue.front_mut().unwrap();
            let additional = num_bytes.saturating_sub(dma.len());
            if dma.try_reclaim(additional) {
                // SAFETY: We won't go over the capacity due to the
                //         assertion at the beginning of the function
                unsafe { dma.set_len(num_bytes) };
                let mem = dma.split_to(num_bytes);

                timer.stop_and_report(
                    Some(Duration::from_micros(2)),
                    "reserving direct-access memory for transfer",
                );
                return mem;
            } else {
                // By rotating the current handle to the end,
                // we reduce the chances of a transfer not being
                // complete by the time we come back around. This
                // will allow us to reclaim the entire buffer later.
                queue.rotate_left(1);
            }
        }
        // Map a new memory zone or die trying!
        // SAFETY: Our device handle is valid and we promise not
        //         to use the memory if the USB device is working
        //         with it.
        let mut dma = unsafe { self.handle.new_usb_mem(DMA_LEN).unwrap() };

        // SAFETY: We won't go over the capacity due to the
        //         assertion at the beginning of the function
        unsafe { dma.set_len(num_bytes) };
        let mem = dma.split_to(num_bytes);
        queue.push_front(dma);
        timer.stop_and_report(None, "allocating direct-access memory for transfer");
        mem
    }
}

#[inline]
fn convert_libusb_to_vhci(
    status: rusb::Result<TransferStatus>,
    kind: UrbType,
    seqnum: u32,
    dev_id: msg::UsbDeviceId,
) -> vhci::Status {
    match status {
        Ok(TransferStatus::Completed) => vhci::Status::Success,
        Ok(TransferStatus::Error) => vhci::Status::Error,
        Ok(TransferStatus::TimedOut) => vhci::Status::TimedOut,
        Ok(TransferStatus::Cancelled) => vhci::Status::Canceled,
        Ok(TransferStatus::Stall) | Err(rusb::Error::InvalidParam) => vhci::Status::Stall,
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
            warn! { %err, "({seqnum}) {kind:?} transfer failed on {dev_id:?}" };
            vhci::Status::Error
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
    pub async fn lend(self) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            id: dev_id,
        } = self;

        enum Event {
            RecvFrame(Option<Recv>),
            SendFrame((Header, UrbHeader, Option<UsbMemMut>, BytesMut)),
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
        let mut active_transfers: JoinSet<(Header, UrbHeader, Option<UsbMemMut>, BytesMut)> =
            JoinSet::new();
        let claimed_interfaces: Arc<Mutex<IntSet<u8>>> = Arc::new(Mutex::new(
            IntSet::with_capacity_and_hasher(16, Default::default()),
        ));

        const BUF_LEN: usize = 16 << 10;
        buf_rx.reserve(BUF_LEN);
        let mut scratch_dma = DmaAllocator::with_capacity(5, Arc::clone(&device));
        let mut scratch_buf = BytesMut::with_capacity(BUF_LEN);
        let cached_transfers: Arc<Mutex<BTreeMap<u16, OneOrMany<InnerTransfer>>>> =
            Arc::new(Mutex::new(BTreeMap::new()));

        let result = loop {
            let check_transfer = !active_transfers.is_empty();
            match tokio::select! {
                biased;
                frame = recv_frame(&mut rx, &mut buf_rx) => {
                    Event::RecvFrame(frame?)
                }
                result = active_transfers.join_next(), if check_transfer => {
                    Event::SendFrame(result.unwrap().unwrap())
                }
            } {
                Event::RecvFrame(Some(Recv::Urb((header, urb_frame)))) => {
                    debug!("({}) received new URB", header.seqnum);
                    let time_to_submit = Timer::start();
                    let cancel = CancellationToken::new();
                    cancel_tokens.insert(header.seqnum, cancel.clone());

                    let claimed = Arc::clone(&claimed_interfaces);
                    let handle = Arc::clone(&device);
                    let cache = Arc::clone(&cached_transfers);

                    // This all took less than 2 microseconds
                    let (urb_header, data) = urb_frame.split::<[u8]>();
                    let mut urb_header = urb_header.read();
                    let ctrl = urb_header.ctrl_packet;
                    let mut data = data.into_bytes_mut();
                    let transfer_len = urb_header.actual_transfer_len as usize;
                    let iso_packet_count = urb_header.iso_packet_count as usize;

                    // New idea: reserve the data before we get into the
                    // future so that we don't need a mutex
                    let time_to_reserve_buffers = Timer::start();
                    let (transfer_buf, mut iso_raw_buf) = if UrbType::Ctrl == urb_header.kind {
                        let mut transfer = {
                            let w_length = ctrl.length() as usize;
                            assert_eq!(w_length, transfer_len);
                            let needed =
                                size_of::<ioctl::IocSetupPacket>() + align_to_usize(transfer_len);
                            scratch_dma.reserve(needed)
                        };
                        ctrl.write_to_prefix(&mut transfer).unwrap();
                        if Dir::Out == ctrl.req().dir() {
                            data.as_ref().write_to_suffix(&mut transfer).unwrap();
                        }
                        (
                            transfer.split_to(transfer_len + size_of::<ioctl::IocSetupPacket>()),
                            BytesMut::new(),
                        )
                    } else {
                        match urb_header.endpoint.direction() {
                            Dir::Out => {
                                let transfer = data
                                    .split_to(align_to_usize(transfer_len))
                                    .split_to(transfer_len);
                                let mut dma = {
                                    let needed = align_to_usize(transfer_len);
                                    scratch_dma.reserve(needed).split_to(transfer_len)
                                };
                                dma.as_mut().copy_from_slice(&transfer);
                                time_to_reserve_buffers.stop_and_report(
                                    None,
                                    "reserving and copying buffer space for transfer",
                                );
                                (dma, data)
                            }
                            Dir::In => {
                                let dma = {
                                    let needed = align_to_usize(transfer_len);
                                    scratch_dma.reserve(needed).split_to(transfer_len)
                                };
                                let iso_buf = if UrbType::Iso == urb_header.kind {
                                    let scratch = &mut scratch_buf;
                                    let len = scratch.len();
                                    let needed =
                                        iso_packet_count * size_of::<ioctl::IocIsoPacketData>();
                                    let additional = needed.saturating_sub(len);
                                    scratch.reserve(additional);
                                    // SAFETY: None of this buf is read, only written to at first.
                                    //         Plus we just ensured that we have capacity for this.
                                    unsafe {
                                        scratch.set_len(needed);
                                    }
                                    scratch.split_to(needed)
                                } else {
                                    BytesMut::new()
                                };
                                time_to_reserve_buffers
                                    .stop_and_report(None, "reserving buffer space for transfer");
                                (dma, iso_buf)
                            }
                        }
                    };

                    let fut = async move {
                        // If we're expecting data, then setup the buffers from our ring.
                        // Otherwise, reserve space to write data.
                        // IDEA: Every branch needs to return:
                        //       - The status given by libusb from the result of the transfer
                        //       - A transfer buffer such that 
                        //         `transfer.len() <= urb_header.actual_transfer_len`
                        //       - An iso packet buffer such that
                        //         `iso_pkts.capacity()` is aligned to 8 bytes
                        let (status, mut transfer, iso_pkts) = match urb_header.kind {
                            UrbType::Iso => {
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

                                // trace!("{urb_header:?}");

                                // TODO: There might be an issue with setting up
                                //       iso packet offsets, I'm not sure yet.
                                let num_iso_packets = urb_header.iso_packet_count as usize;
                                let iso_packets =
                                    <[ioctl::IocIsoPacketData]>::ref_from_bytes_with_elems(
                                        &iso_raw_buf[..],
                                        num_iso_packets,
                                    )
                                    .unwrap();
                                // trace!("{:?}", &iso_packets);

                                let transfer = get_or_alloc_transfer(cache.lock().unwrap(), num_iso_packets as u16);
                                let mut transfer = unsafe {
                                    transfer.into_iso(
                                        &handle,
                                        urb_header.endpoint.0,
                                        transfer_buf,
                                        Iter {
                                            pkts: iso_packets.iter(),
                                        },
                                    )
                                };

                                time_to_submit.stop_and_report(None, "setting up iso transfer");
                                let result = transfer.submit(cancel).await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                // let _errno = dbg!(io::Error::last_os_error());

                                let iso_packets =
                                    <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                                        &mut iso_raw_buf[..],
                                        num_iso_packets,
                                    )
                                    .unwrap();
                                for (our_pkt, libusb_pkt) in
                                    iso_packets.iter_mut().zip(transfer.iso_packets().unwrap())
                                {
                                    our_pkt.packet_actual = libusb_pkt.actual_len();
                                    our_pkt.status = libusb_pkt.status() as i32;
                                }

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.lock().unwrap(), transfer);
                                (status, Some(buf), iso_raw_buf)
                            }
                            UrbType::Int => {
                                let transfer = get_or_alloc_transfer(cache.lock().unwrap(), 0);
                                let mut transfer = unsafe {
                                    transfer.into_int(
                                        &handle,
                                        urb_header.endpoint.0,
                                        transfer_buf,
                                    )
                                };

                                time_to_submit.stop_and_report(None, "setting up int transfer");
                                let result = transfer.submit(cancel).await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.lock().unwrap(), transfer);
                                (status, Some(buf), BytesMut::new())
                            }
                            UrbType::Ctrl
                                if Request::STANDARD_INTERFACE_SET_INTERFACE == ctrl.req() =>
                            {
                                let timer = Timer::start();
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
                                timer.stop_and_report(None, "setting alternate setting");

                                (status, None, BytesMut::new())
                            }
                            UrbType::Ctrl
                                if Request::STANDARD_DEVICE_SET_CONFIGURATION == ctrl.req()
                                    && is_config_active(&handle, ctrl.value() as u8) =>
                            {
                                debug!("({}) config {} is already set", header.seqnum, ctrl.value() as u8);
                                let status = vhci::Status::Success;
                                (status, None, BytesMut::new())
                            }
                            UrbType::Ctrl
                                if Request::STANDARD_DEVICE_SET_CONFIGURATION == ctrl.req()
                                    && !is_config_active(&handle, ctrl.value() as u8) =>
                            {
                                let desired = ctrl.value() as u8;
                                let set_config = tokio::task::spawn_blocking(move || set_config(header.seqnum, desired, claimed.lock().unwrap(), &handle));

                                let status = set_config.await.unwrap();

                                (status, None, BytesMut::new())
                            }
                            UrbType::Ctrl if Request::STANDARD_ENDPOINT_CLEAR_FEATURE == ctrl.req() => {
                                let endpoint = ctrl.index() as u8;

                                let timer = Timer::start();
                                let status = match handle.clear_halt(endpoint) {
                                    Ok(_) => vhci::Status::Success,
                                    Err(err) => {
                                        warn! {
                                            %err,
                                            "({}) couldn't clear stall for endpoint {}",
                                            header.seqnum,
                                            endpoint,
                                        };
                                        vhci::Status::Stall
                                    },
                                };
                                timer.stop_and_report(None, "clearing stall");

                                (status, None, BytesMut::new())
                            }
                            UrbType::Ctrl => {
                                let is_get_status = Request::STANDARD_DEVICE_GET_STATUS == ctrl.req();
                                let transfer = get_or_alloc_transfer(cache.lock().unwrap(), 0);
                                // SAFETY: Transfer buffer is longer than
                                //         required lengths, and setup packet
                                //         contains the right length as well.
                                let mut transfer = unsafe {
                                    transfer.into_ctrl(
                                        &handle,
                                        transfer_buf,
                                        Duration::from_millis(900),
                                    )
                                };

                                time_to_submit.stop_and_report(None, "setting up ctrl transfer");
                                let result = transfer.submit(cancel).await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                trace! { %ctrl };

                                let mut buf = {
                                    let (transfer, mut buf) = transfer.into_parts().unwrap();
                                    insert_spare_transfer(cache.lock().unwrap(), transfer);
                                    buf.split_off(size_of::<ioctl::IocSetupPacket>())
                                };

                                if is_get_status {
                                    // This sets the lowest bit to 1, which
                                    // indicates that our fake USB device is self powered.
                                    buf[0] = 0x01;
                                }
                                (status, Some(buf), BytesMut::new())
                            }
                            UrbType::Bulk => {
                                let transfer = get_or_alloc_transfer(cache.lock().unwrap(), 0);
                                let mut transfer = unsafe {
                                    transfer.into_bulk(
                                        &handle,
                                        urb_header.endpoint.0,
                                        TransferFlags::NONE,
                                        transfer_buf,
                                    )
                                };

                                time_to_submit.stop_and_report(Some(Duration::from_micros(15)), "setting up bulk transfer");
                                let result = transfer.submit(cancel).await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.lock().unwrap(), transfer);
                                (status, Some(buf), BytesMut::new())
                            }
                        };

                        urb_header.status = status;
                        if Dir::Out == urb_header.endpoint.direction()
                        {
                            let len = transfer.as_ref().map(|t| t.len()).unwrap_or_default();
                            if len != urb_header.actual_transfer_len as usize {
                                warn!(
                                    "({}) did not finish transferring data ({}/{})", 
                                    header.seqnum,
                                    len,
                                    urb_header.actual_transfer_len
                                );
                                urb_header.actual_transfer_len = len as u16;
                            }
                            if let Some(t) = transfer.as_mut() {
                                t.clear();
                            }
                        }

                        (header, urb_header, transfer, iso_pkts)
                    }.in_current_span();
                    // debug!("size_of::<F>() = {}", size_of_val(&fut));
                    active_transfers.spawn(fut);
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
                    let timer = Timer::start();
                    cancel_tokens.remove(&header.seqnum);

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

                    let transfer = transfer.as_ref().map(|t| t.as_ref()).unwrap_or_default();
                    let scratch = [0u8; 7];
                    let padding = {
                        let padded_len = align_to_usize(transfer.len()) - transfer.len();
                        &scratch[..padded_len]
                    };
                    let transfer_padded_len = transfer.len() + padding.len();
                    debug_assert_eq!(transfer_padded_len % 8, 0);

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
                        timer.stop_and_report(
                            Some(Duration::from_nanos(500)),
                            "setting up response frame",
                        );
                        tx.write_all_buf(&mut response).await?;
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
                    } else {
                        let mut response = header.as_bytes();
                        timer.stop_and_report(None, "setting up response frame");
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
    pub async fn send_device_list<'a, I, T>(self, iter: impl Fn() -> io::Result<I>) -> Result<()>
    where
        I: Iterator<Item = T>,
        T: msg::SendUsbDeviceInfo,
    {
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

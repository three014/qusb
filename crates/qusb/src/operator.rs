use std::{
    cell::{RefCell, RefMut},
    collections::{BTreeMap, VecDeque},
    io,
    ops::DerefMut,
    rc::Rc,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
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
use tracing::{debug, error, info, trace, trace_span, warn, warn_span, Instrument};
use vhci::{
    ioctl::{self, UrbType},
    usbfs::{Dir, Request},
    DataRate, PortChange, PortFlag, PortStatus,
};
use zerocopy::{FromBytes, FromZeros, IntoBytes, TryFromBytes};

use crate::{
    stub::{self, RegisterPort},
    utils::{self, align_to_usize, SimpleMap},
    Error, Result, UrbWithIsoData, UrbWithIsoGiveback,
};

mod borrow;

enum Recv {
    Urb((Header, Data<UrbFrame>)),
    PortReset(Header),
    Unlink(Header),
}

async fn recv_frame<R: AsyncRead + Unpin>(mut rx: R, buf: &mut Ring) -> io::Result<Option<Recv>> {
    let mut min_len = size_of::<Header>();
    let frame: Data<QusbFrame> = loop {
        if buf.fill_until(&mut rx, min_len).await?.is_none() {
            return Ok(None);
        }

        min_len = match buf.claim_dst() {
            Ok(frame) => break frame,
            Err(ReadError::CorruptedData) => todo!(),
            Err(ReadError::BufferShort { num_bytes_needed }) => buf.len() + num_bytes_needed,
        }
    };

    let frame_ref = frame.get();
    match frame_ref.header.command {
        msg::Command::CmdUnlink => Ok(Some(Recv::Unlink(frame_ref.header))),
        msg::Command::CmdPort | msg::Command::RetPort => {
            Ok(Some(Recv::PortReset(frame_ref.header)))
        }
        msg::Command::RetSubmit | msg::Command::CmdSubmit => {
            // In this case this will probably
            // be faster since we already parsed
            // the header
            let header = frame_ref.header;
            let (_, urb) = frame.split::<UrbFrame>();
            Ok(Some(Recv::Urb((header, urb))))
        }
        _ => unreachable!("client smh smh"),
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
            "remote dev {:03}/{:03} local port {}",
            self.remote_dev.bus_number,
            self.remote_dev.device_addr,
            self.local_port.get()
        )
    }
}

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
        self.ioctl_urb.endpoint.direction()
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
    fn iso_packet_giveback_mut(&mut self) -> &mut [ioctl::IocIsoPacketGiveback] {
        &mut []
    }

    fn error_count(&self) -> u16 {
        0
    }
}

struct BorrowSendHandler {
    buf: BytesMut,
    vhci: stub::VhciRemote,
    id: BorrowId,
    cur_seq: AtomicU32,
    prev: ioctl::IocPortStat,
    addr: u8,
    handle_seqnum_map: Arc<
        Mutex<(
            SimpleMap<ioctl::UrbHandle, u32>,
            SimpleMap<u32, ioctl::UrbHandle>,
        )>,
    >,
}

impl BorrowSendHandler {
    pub fn new(
        vhci: stub::VhciRemote,
        id: BorrowId,
        handle_seqnum_map: Arc<
            Mutex<(
                SimpleMap<ioctl::UrbHandle, u32>,
                SimpleMap<u32, ioctl::UrbHandle>,
            )>,
        >,
    ) -> Self {
        const BUF_LEN: usize = 16 << 12;
        Self {
            buf: BytesMut::with_capacity(BUF_LEN),
            vhci,
            id,
            cur_seq: AtomicU32::new(0),
            prev: ioctl::IocPortStat::default(),
            addr: 0,
            handle_seqnum_map,
        }
    }
}

impl borrow::SendHandler for BorrowSendHandler {
    #[tracing::instrument(level = "trace", skip_all)]
    fn port_stat(&mut self, next: ioctl::IocPortStat) -> Option<Header> {
        let status = next.status();
        let change = next.change();
        let flags = next.flags();
        if change.contains(PortChange::CONNECTION) {
            debug!("CONNECTION state changed -> invalidating address");
            self.addr = 0xff;
        } else if change.contains(PortChange::RESET)
            && (!status).contains(PortStatus::RESET)
            && status.contains(PortStatus::ENABLE)
        {
            debug!("RESET successful -> use default address");
            self.addr = 0;
        } else if self.prev.status().contains(PortStatus::POWER)
            && (!status).contains(PortStatus::POWER)
        {
            // debug!("port is powered off");
        } else if (!self.prev.status()).contains(PortStatus::RESET)
            && status.contains(PortStatus::RESET | PortStatus::CONNECTION)
        {
            let next_seqnum = self.cur_seq.fetch_add(1, Ordering::Relaxed);
            // We pray that we don't run into another handle
            let handle = ioctl::UrbHandle(rand::random());
            {
                let mut guard = self.handle_seqnum_map.lock().unwrap();
                let (seqnums, handles) = guard.deref_mut();
                seqnums.insert(handle, next_seqnum);
                handles.insert(next_seqnum, handle);
            }
            debug!("({next_seqnum}) port is resetting");

            let header = Header {
                total_frame_len: (size_of::<Header>() / 8) as u16,
                seqnum: next_seqnum,
                command: msg::Command::CmdPort,
                status: msg::Status::Success,
            };

            return Some(header);
        } else if (!self.prev.flags()).contains(PortFlag::RESUMING)
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

        None
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[inline]
    async fn set_address(
        &mut self,
        urb: ioctl::IocUrb,
        handle: ioctl::UrbHandle,
    ) -> io::Result<()> {
        self.addr = urb.setup_packet.value() as u8;
        debug!("({}) set local dev address to {:03}", self.id, self.addr);

        let urb = EmptyUrb {
            handle,
            ioctl_urb: urb,
        };

        self.vhci.giveback_urb(urb).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn cancel_urb(&mut self, handle: ioctl::UrbHandle) -> Option<Header> {
        debug!("({}) got cancel urb ({handle:?})", self.id);
        let maybe = {
            let guard = self.handle_seqnum_map.lock().unwrap();
            guard.0.get(&handle).copied()
        };
        if let Some(seqnum) = maybe {
            let header = Header {
                total_frame_len: (size_of::<Header>() / 8) as u16,
                seqnum,
                command: msg::Command::CmdUnlink,
                status: msg::Status::Success,
            };

            Some(header)
        } else {
            debug!("{handle:?} had already been returned");
            None
        }
    }

    fn process_urb(&mut self, urb: ioctl::IocUrb, handle: ioctl::UrbHandle) -> io::Result<Bytes> {
        assert_eq!(self.addr, urb.address.get());

        let next_seq = self.cur_seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut guard = self.handle_seqnum_map.lock().unwrap();
            let (seqnums, handles) = guard.deref_mut();
            assert!(handles.insert(next_seq, handle).is_none());
            assert!(seqnums.insert(handle, next_seq).is_none());
        }

        // Calculating all parts of the transfer frame
        let actual_transfer_len = urb.buffer_length as usize;
        let packet_count = urb.packet_count as usize;

        const URB_NO_TRANSFER_DMA_MAP: u16 = 0x04;
        let urb_header = UrbHeader {
            kind: urb.typ,
            actual_transfer_len: actual_transfer_len as u16,
            iso_packet_count: packet_count as u16,
            interval: urb.interval as u16,
            // Remove URB_NO_TRANSFER_DMA_MAP flag
            flags: urb.flags & !URB_NO_TRANSFER_DMA_MAP,
            endpoint: urb.endpoint,
            num_errors: 0,
            status: vhci::Status::Pending,
            ctrl_packet: urb.setup_packet,
        };

        let is_out = urb_header.is_out();
        let needs_fetch = (actual_transfer_len > 0 && is_out) || packet_count > 0;
        let real_transfer_len = urb_header.padded_transfer_len() * is_out as usize;
        let iso_byte_len = urb_header.iso_byte_len();
        let header_len = size_of::<Header>() + size_of::<UrbHeader>();
        let data_len = real_transfer_len + iso_byte_len;
        let total_frame_len = header_len + data_len;

        let mut buf = {
            let additional = total_frame_len.saturating_sub(self.buf.len());
            self.buf.reserve(additional);
            // SAFETY: Data will be initialized right after this call.
            unsafe {
                self.buf.set_len(total_frame_len);
            }
            self.buf.split_to(total_frame_len)
        };

        if needs_fetch {
            let data = &mut buf[header_len..header_len + data_len];
            let (transfer, iso_data) = if is_out {
                // This is an OUT transfer, therefore we have already reserved
                // the right buffer size for the data. Now we just split the
                // buffer into the transfer and iso data.
                let (transfer, rest) = data.split_at_mut(real_transfer_len);
                let iso_data =
                    <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(rest, packet_count)
                        .unwrap();
                (transfer, iso_data)
            } else {
                // This is an Isochronous IN transfer, therefore we don't need
                // to grab the transfer data since there is none.
                // So we use the rest of the scratch space as buffer!
                let additional = actual_transfer_len.saturating_sub(self.buf.len());
                self.buf.reserve(additional);

                // SAFETY: Data will never be read.
                unsafe { self.buf.set_len(actual_transfer_len) };
                let transfer = &mut self.buf[..actual_transfer_len];
                // CONTRACT: All of `data` is the isochronous packet buffer.
                let iso_data =
                    <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(data, packet_count)
                        .unwrap();
                (transfer, iso_data)
            };

            let borrower_urb = UrbWithIsoData {
                handle,
                header: &urb_header,
                transfer: &mut transfer[..actual_transfer_len],
                iso_data,
            };

            self.vhci.fetch_data(borrower_urb)?;
        }

        let header = Header {
            total_frame_len: (total_frame_len / 8) as u16,
            seqnum: next_seq,
            command: msg::Command::CmdSubmit,
            status: msg::Status::Success,
        };

        // Finally, we write the two headers into the beginning of the reserved buffer.
        // First we zero out those bytes so we can safely access the slice
        buf[..header_len].zero();

        // Now we grab our references and assign our headers to them.
        let (header_ref, rest) = Header::try_mut_from_prefix(&mut buf[..header_len]).unwrap();
        *header_ref = header;
        let urb_ref = UrbHeader::try_mut_from_bytes(rest).unwrap();
        *urb_ref = urb_header;

        // Finally we can return the completed buffer.
        Ok(buf.freeze())
    }
}

struct BorrowRecvHandler {
    vhci: stub::VhciRemote,
    id: BorrowId,
    handle_seqnum_map: Arc<
        Mutex<(
            SimpleMap<ioctl::UrbHandle, u32>,
            SimpleMap<u32, ioctl::UrbHandle>,
        )>,
    >,
}

impl BorrowRecvHandler {
    pub fn new(
        vhci: stub::VhciRemote,
        id: BorrowId,
        handle_seqnum_map: Arc<
            Mutex<(
                SimpleMap<ioctl::UrbHandle, u32>,
                SimpleMap<u32, ioctl::UrbHandle>,
            )>,
        >,
    ) -> Self {
        Self {
            vhci,
            id,
            handle_seqnum_map,
        }
    }
}

impl borrow::RecvHandler for BorrowRecvHandler {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn urb_reply(&mut self, seqnum: u32, mut data: Data<UrbFrame>) -> io::Result<()> {
        let handle = {
            let mut guard = self.handle_seqnum_map.lock().unwrap();
            let (seqnums, handles) = guard.deref_mut();
            let handle = handles.remove(&seqnum).unwrap();
            _ = seqnums.remove(&handle).unwrap();
            handle
        };
        let frame = data.get_mut();
        let urb = frame.header();

        if vhci::Status::Success != urb.status {
            warn!(
                "({}) {:?} {:?} transfer {seqnum} failed: {:?}",
                self.id,
                urb.kind,
                urb.endpoint.direction(),
                urb.status
            );
        }
        let actual_transfer_len = urb.actual_transfer_len as usize;

        // We might not be expecting data if we sent some to the usb device
        let (transfer, rest) = match urb.endpoint.direction() {
            Dir::Out => (Default::default(), &mut frame.data),
            Dir::In => {
                let (transfer, rest) =
                    <[u8]>::mut_from_prefix_with_elems(&mut frame.data, urb.padded_transfer_len())
                        .unwrap();
                (&mut transfer[..actual_transfer_len], rest)
            }
        };

        let iso_giveback = <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
            rest,
            urb.iso_packet_count as usize,
        )
        .unwrap();

        let lender_urb = UrbWithIsoGiveback {
            handle,
            header: &urb,
            transfer,
            iso_giveback,
        };

        self.vhci.giveback_urb(lender_urb).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn device_reset(&mut self, seqnum: u32) -> io::Result<()> {
        {
            let mut guard = self.handle_seqnum_map.lock().unwrap();
            let (seqnums, handles) = guard.deref_mut();
            let handle = handles.remove(&seqnum).unwrap();
            _ = seqnums.remove(&handle).unwrap();
        }
        debug!("({}) port has been reset", self.id);
        self.vhci.reset_done(self.id.local_port, true)
    }
}

/// A struct containing the logic for
/// borrowing a USB device from a lender.
pub struct BorrowDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    vhci: stub::Controller,
    data_rate: msg::DataRate,
    id: msg::UsbDeviceId,
}

impl<W, R> BorrowDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        vhci: stub::Controller,
        data_rate: msg::DataRate,
        id: msg::UsbDeviceId,
    ) -> Self {
        Self {
            tx,
            rx,
            buf_rx,
            vhci,
            data_rate,
            id,
        }
    }
}

impl<W, R> BorrowDevice<W, R>
where
    W: AsyncWrite + utils::CloseStream + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
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
    pub async fn borrow(self, cancel_token: CancellationToken) -> Result<()> {
        let Self {
            tx,
            rx,
            mut buf_rx,
            mut vhci,
            data_rate,
            id: dev_id,
        } = self;

        buf_rx.reserve(8192);

        let data_rate = match data_rate {
            msg::DataRate::Low => DataRate::Low,
            msg::DataRate::Full => DataRate::Full,
            msg::DataRate::High => DataRate::High,
        };
        let (port, work_rx) = vhci.register(RegisterPort::Any, data_rate).await?;

        let id = BorrowId {
            remote_dev: dev_id,
            local_port: port,
        };
        info!("({id}) connected new device with {data_rate:?} speed");

        let map = Arc::new(Mutex::new((
            SimpleMap::with_capacity_and_hasher(512, Default::default()),
            SimpleMap::with_capacity_and_hasher(512, Default::default()),
        )));
        let cloned_map = Arc::clone(&map);
        let send_loop = borrow::SendLoop::new(tx, work_rx);
        let send_handler = BorrowSendHandler::new(vhci.remote(), id, map);
        let send = tokio::spawn(send_loop.run(send_handler, cancel_token.clone()));
        let recv_loop = borrow::RecvLoop::new(rx, buf_rx);
        let recv_handler = BorrowRecvHandler::new(vhci.remote(), id, cloned_map);
        let recv = tokio::spawn(recv_loop.run(recv_handler, cancel_token.clone()));

        let recv_result = recv.await.unwrap();
        cancel_token.cancel();
        let send_result = send.await.unwrap();

        info!("disconnecting {id}");
        vhci.disconnect(port).await?;
        send_result?;
        recv_result?;
        Ok(())
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

#[tracing::instrument(level = "debug", skip_all)]
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

pub(crate) fn open_device(
    dev_id: msg::UsbDeviceId,
) -> rusb::Result<rusb::DeviceHandle<rusb::Context>> {
    rusb::Context::new()
        // .and_then(|mut ctx| {
        //     let span = tracing::trace_span!("libusb");
        //     ctx.set_log_level(rusb::LogLevel::Debug);
        //     ctx.set_log_callback(
        //         Box::new(move |_level, msg| {
        //             let _enter = span.enter();
        //             tracing::trace!("{}", msg.trim_end());
        //         }),
        //         rusb::LogCallbackMode::Context,
        //     );
        //     ctx.devices()
        // })
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
            Ok(handle)
        })
}

#[tracing::instrument(level = "debug", skip_all)]
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

fn get_or_alloc_transfer(
    mut cache: RefMut<'_, BTreeMap<u16, OneOrMany<InnerTransfer>>>,
    num_iso_packets: u16,
) -> InnerTransfer {
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
        transfer
    } else {
        drop(cache);
        InnerTransfer::new(num_iso_packets as usize)
    }
}

fn insert_spare_transfer(
    mut cache: RefMut<'_, BTreeMap<u16, OneOrMany<InnerTransfer>>>,
    transfer: InnerTransfer,
) {
    const EMPTY: OneOrMany<InnerTransfer> = OneOrMany::Many(Vec::new());
    let entry = cache.get_mut(&(transfer.num_iso_packets() as u16));
    match entry {
        Some(entry) => match std::mem::replace(entry, EMPTY) {
            OneOrMany::One(other) => {
                *entry = OneOrMany::Many(vec![transfer, other]);
            }
            OneOrMany::Many(mut vec) => {
                vec.push(transfer);
                *entry = OneOrMany::Many(vec);
            }
        },
        // Almost all our transfers will be made for Ctrl, Int, or Bulk, so we go ahead
        // and store those transfers in a Vec as a small optimization
        None if 0 == transfer.num_iso_packets() => {
            cache.insert(
                transfer.num_iso_packets() as u16,
                OneOrMany::Many(vec![transfer]),
            );
        }
        None => {
            cache.insert(transfer.num_iso_packets() as u16, OneOrMany::One(transfer));
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

    pub fn reserve(&mut self, num_bytes: usize) -> UsbMemMut {
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
        mem
    }
}

#[inline]
const fn vhci_from_transfer_status(status: TransferStatus) -> vhci::Status {
    match status {
        TransferStatus::Completed => vhci::Status::Success,
        TransferStatus::Error => vhci::Status::Error,
        TransferStatus::TimedOut => vhci::Status::TimedOut,
        TransferStatus::Cancelled => vhci::Status::Canceled,
        TransferStatus::Stall => vhci::Status::Stall,
        TransferStatus::NoDevice => vhci::Status::DeviceDisconnected,
        TransferStatus::Overflow => vhci::Status::Babble,
    }
}

#[inline]
fn convert_libusb_to_vhci(
    status: rusb::Result<TransferStatus>,
    kind: UrbType,
    seqnum: u32,
    dev_id: msg::UsbDeviceId,
) -> vhci::Status {
    let _guard = tracing::Span::current().entered();
    match status {
        Ok(status) => vhci_from_transfer_status(status),
        Err(rusb::Error::InvalidParam) => vhci::Status::Stall,
        Err(rusb::Error::NoDevice) => vhci::Status::DeviceDisconnected,
        Err(rusb::Error::Busy) => {
            unreachable!("for now, no transfer can be resubmitted")
        }
        Err(rusb::Error::NotSupported) => {
            unreachable!("will we ever mess with the transfer flags?")
        }
        Err(err) => {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap();
            warn! { %err, %errno, "({seqnum}) {kind:?} transfer failed on {dev_id:?}" };
            vhci::Status::from_errno_raw(-errno, UrbType::Iso == kind)
        }
    }
}

#[inline]
fn write_transfer(src: &[u8], dst: &mut [u8]) {
    let src = <[u64]>::ref_from_bytes(src).unwrap();
    let dst = <[u64]>::mut_from_bytes(dst).unwrap();
    dst.copy_from_slice(src);
}

type TransferPayload = (Header, UrbHeader, Option<UsbMemMut>, BytesMut);

pub struct LendDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    device: rusb::DeviceHandle<rusb::Context>,
    id: msg::UsbDeviceId,
}

impl<W, R> LendDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        device: rusb::DeviceHandle<rusb::Context>,
        id: msg::UsbDeviceId,
    ) -> Self {
        Self {
            tx,
            rx,
            buf_rx,
            device,
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
    pub async fn lend(self, cancel_token: CancellationToken) -> Result<()> {
        let Self {
            mut tx,
            mut rx,
            mut buf_rx,
            device,
            id: dev_id,
        } = self;

        enum Event {
            RecvFrame(Option<Recv>),
            SendFrame(TransferPayload),
            Cancelled,
        }

        let device = Arc::new(device);
        let event_handler = CancellationToken::new();
        let runtime = event_handler.clone();
        let ctx = device.context().clone();
        let event_handler_loop = std::thread::spawn(move || {
            let _guard = warn_span!("libusb_event_handler").entered();
            while !runtime.is_cancelled() {
                if let Err(err) = ctx.handle_events(Some(Duration::from_secs(5))) {
                    warn! { %err };
                }
            }
        });

        let mut cancel_tokens: IntMap<u32, CancellationToken> =
            IntMap::with_capacity_and_hasher(1024, Default::default());
        let mut active_transfers: JoinSet<TransferPayload> = JoinSet::new();
        let claimed_interfaces: Arc<Mutex<IntSet<u8>>> = Arc::new(Mutex::new(
            IntSet::with_capacity_and_hasher(16, Default::default()),
        ));

        const BUF_LEN: usize = 16 << 10;
        buf_rx.reserve(BUF_LEN);
        let mut scratch_dma = DmaAllocator::with_capacity(5, Arc::clone(&device));
        let cached_transfers: Rc<RefCell<BTreeMap<u16, OneOrMany<InnerTransfer>>>> =
            Rc::new(RefCell::new(BTreeMap::new()));

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
                _ = cancel_token.cancelled() => {
                    Event::Cancelled
                }
            } {
                Event::RecvFrame(Some(Recv::Urb((header, urb_frame)))) => {
                    let cancel = CancellationToken::new();
                    cancel_tokens.insert(header.seqnum, cancel.clone());

                    let claimed = Arc::clone(&claimed_interfaces);
                    let handle = Arc::clone(&device);
                    let cache = cached_transfers.clone();

                    let (urb_header, data) = urb_frame.split::<[u8]>();
                    let mut urb_header = urb_header.read();
                    let ctrl = urb_header.ctrl_packet;
                    let mut data = data.into_bytes_mut();
                    let transfer_len = urb_header.actual_transfer_len as usize;

                    // Reserve the data before we get into the
                    // future so that we don't need synchronization.
                    let (transfer_buf, mut iso_raw_buf) = if UrbType::Ctrl == urb_header.kind {
                        let mut transfer = {
                            let w_length = ctrl.length() as usize;
                            assert_eq!(w_length, transfer_len);
                            let needed =
                                size_of::<ioctl::IocSetupPacket>() + align_to_usize(transfer_len);
                            scratch_dma.reserve(needed)
                        };
                        *ioctl::IocSetupPacket::mut_from_bytes(
                            &mut transfer[..size_of::<ioctl::IocSetupPacket>()],
                        )
                        .unwrap() = ctrl;
                        if Dir::Out == ctrl.req().dir() {
                            write_transfer(
                                &data,
                                &mut transfer[size_of::<ioctl::IocSetupPacket>()..],
                            );
                        }
                        (
                            transfer.split_to(transfer_len + size_of::<ioctl::IocSetupPacket>()),
                            BytesMut::new(),
                        )
                    } else {
                        let transfer_buf = match urb_header.endpoint.direction() {
                            Dir::Out => {
                                let needed = align_to_usize(transfer_len);
                                let transfer = data.split_to(needed);
                                let mut dma = scratch_dma.reserve(needed);
                                write_transfer(&transfer, &mut dma);
                                dma.split_to(transfer_len)
                            }
                            Dir::In => {
                                let needed = align_to_usize(transfer_len);
                                scratch_dma.reserve(needed).split_to(transfer_len)
                            }
                        };
                        (transfer_buf, data)
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

                                // TODO: There might be an issue with setting up
                                //       iso packet offsets, I'm not sure yet.
                                let num_iso_packets = urb_header.iso_packet_count as usize;
                                let iso_packets =
                                    <[ioctl::IocIsoPacketData]>::ref_from_bytes_with_elems(
                                        &iso_raw_buf[..],
                                        num_iso_packets,
                                    )
                                    .unwrap();

                                let transfer = get_or_alloc_transfer(cache.borrow_mut(), num_iso_packets as u16);
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

                                // SAFETY: TODO: Ensure that tokio completes all transfers.
                                let result = unsafe { transfer.submit(cancel) }.await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                let is_enoent = vhci::Status::Canceled == status && result.is_err_and(|err| rusb::Error::Io == err);

                                let our_packets =
                                    <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                                        &mut iso_raw_buf[..],
                                        num_iso_packets,
                                    )
                                    .unwrap();
                                let their_packets = transfer.iso_packets().expect("why wouldn't a transfer be complete here??");

                                for (our_pkt, libusb_pkt) in our_packets.iter_mut().zip(their_packets)
                                {
                                    our_pkt.packet_actual = libusb_pkt.actual_len();
                                    our_pkt.status = if !is_enoent {
                                        vhci_from_transfer_status(libusb_pkt.status()).to_errno_raw(true)
                                    } else {
                                        vhci::Status::Pending.to_errno_raw(true)
                                    };
                                }
                                let (transfer, mut buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.borrow_mut(), transfer);

                                if Dir::In == urb_header.endpoint.direction() {
                                    // SAFETY: Isochronous transfer requires that full
                                    //         buffer sent back to caller.
                                    unsafe { buf.set_len(buf.capacity()) };
                                }

                                (status, Some(buf), iso_raw_buf)
                            }
                            UrbType::Int => {
                                let transfer = get_or_alloc_transfer(cache.borrow_mut(), 0);
                                let mut transfer = unsafe {
                                    transfer.into_int(
                                        &handle,
                                        urb_header.endpoint.0,
                                        transfer_buf,
                                    )
                                };

                                // SAFETY: TODO: Ensure that tokio completes all transfers.
                                let result = unsafe { transfer.submit(cancel) }.await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.borrow_mut(), transfer);
                                (status, Some(buf), BytesMut::new())
                            }
                            UrbType::Ctrl
                                if Request::STANDARD_INTERFACE_SET_INTERFACE == ctrl.req() =>
                            {
                                let setting = ctrl.value() as u8;
                                let interface = ctrl.index() as u8;
                                trace!("using setting {setting} for interface {interface}");
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

                                (status, None, BytesMut::new())
                            }
                            UrbType::Ctrl => {
                                let is_get_status = Request::STANDARD_DEVICE_GET_STATUS == ctrl.req();
                                let transfer = get_or_alloc_transfer(cache.borrow_mut(), 0);
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

                                // SAFETY: TODO: Ensure that tokio completes all transfers.
                                let result = unsafe { transfer.submit(cancel) }.await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                let mut buf = {
                                    let (transfer, mut buf) = transfer.into_parts().unwrap();
                                    insert_spare_transfer(cache.borrow_mut(), transfer);
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
                                let transfer = get_or_alloc_transfer(cache.borrow_mut(), 0);
                                let mut transfer = unsafe {
                                    transfer.into_bulk(
                                        &handle,
                                        urb_header.endpoint.0,
                                        TransferFlags::NONE,
                                        transfer_buf,
                                    )
                                };

                                // SAFETY: TODO: Ensure that tokio completes all transfers.
                                let result = unsafe { transfer.submit(cancel) }.await;
                                let status = convert_libusb_to_vhci(result, urb_header.kind, header.seqnum, dev_id);

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.borrow_mut(), transfer);
                                (status, Some(buf), BytesMut::new())
                            }
                        };

                        urb_header.status = status;
                        if Dir::Out == urb_header.endpoint.direction()
                        {
                            let len = transfer.as_ref().map(|t| t.len()).unwrap_or_default();
                            if len != urb_header.actual_transfer_len as usize {
                                if UrbType::Iso != urb_header.kind {
                                    warn!(
                                        "({}) {:?} did not finish transferring data ({}/{})", 
                                        header.seqnum,
                                        urb_header.kind,
                                        len,
                                        urb_header.actual_transfer_len
                                    );
                                }
                                urb_header.actual_transfer_len = len as u16;
                            }
                            if let Some(t) = transfer.as_mut() {
                                t.clear();
                            }
                        }
                        if UrbType::Ctrl == urb_header.kind {
                            trace! { %ctrl };
                        }

                        (header, urb_header, transfer, iso_pkts)
                    }.instrument(trace_span!("transfer"));
                    // debug!("size_of::<F>() = {}", size_of_val(&fut));
                    active_transfers.spawn_local(fut);
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
                    cancel_tokens.remove(&header.seqnum);

                    let status = match urb_header.status {
                        vhci::Status::Pending => todo!(),
                        // vhci::Status::Error => msg::Status::DevErr,
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

                    let dir = urb_header.endpoint.direction();

                    let actual_transfer_len = if Dir::Out == dir {
                        urb_header.actual_transfer_len
                    } else {
                        transfer.len() as u16
                    };

                    let iso_packet_count =
                        (iso_pkts.len() / size_of::<ioctl::IocIsoPacketGiveback>()) as u16;

                    let total_frame_len = if Dir::Out == dir {
                        size_of::<Header>() + size_of::<UrbHeader>() + iso_pkts.len()
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
                        tx.write_all_buf(&mut response).await?;
                    } else {
                        error!(
                            "({}) {:?} on {:?} failed, {:?}",
                            header.seqnum, urb.kind, urb.endpoint, urb.status
                        );
                        let response = Header {
                            total_frame_len: (size_of::<Header>() / 8) as u16,
                            ..header
                        };
                        tx.write_all_buf(&mut response.as_bytes()).await?;
                        let errno = urb.status.to_errno_raw(UrbType::Iso == urb.kind);
                        break Err(Error::Io(io::Error::from_raw_os_error(-errno)));
                    }
                }
                Event::RecvFrame(None) | Event::Cancelled => break Ok(()),
            }
        };

        info!(
            "shutting down ({:03}/{:03})",
            dev_id.bus_number, dev_id.device_addr
        );
        cancel_tokens.into_values().for_each(|token| token.cancel());
        _ = active_transfers.join_all().await;
        event_handler.cancel();
        drop(device);
        _ = event_handler_loop.join();
        _ = tx.close().await;

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
                let response = msg::Resp::ListDevices {
                    _padding: Default::default(),
                };
                msg::send_resp(&mut tx, response).await?;
                devices
            }
            Err(err) => {
                let response = msg::Resp::Failure {
                    stat: msg::Status::Failed,
                    ver: msg::VersionOpt::None(Default::default()),
                };
                msg::send_resp(&mut tx, response).await?;
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

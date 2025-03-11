use std::{
    cell::{RefCell, RefMut},
    collections::{BTreeMap, VecDeque},
    io,
    ops::DerefMut,
    pin::pin,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use compio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    runtime::time::sleep,
};
use futures_concurrency::{future::Race, stream::Merge};
use futures_lite::{stream, StreamExt};
use lend::{blocking::BlockingOps, Bulk, Ctrl, Int, Iso, ResultData};
use nohash_hasher::{IntMap, IntSet};
use proto::{
    data::{Data, ReadError, Ring},
    msg::{self, Header, QusbFrame, UrbFrame, UrbHeader},
};
use rusb::UsbContext;
use rusb_async::{
    DeviceHandleExt, InnerTransfer, IsoPacket, TransferFlags, TransferStatus, UsbMemMut,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, trace_span, warn, warn_span, Instrument};
use vhci::{
    ioctl::{self, UrbType},
    usbfs::{Dir, Request},
    DataRate, PortChange, PortFlag, PortStatus,
};
use zerocopy::{transmute, FromBytes, IntoBytes};

use crate::{
    stub::{self, RegisterPort},
    utils::{align_to_usize, metrics, mpsc, Counter, SimpleMap},
    Error, Result, UrbWithIsoData,
};

mod borrow;
mod lend;

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
            Err(ReadError::CorruptedData) => Err(io::Error::other("corrupted data from peer"))?,
            Err(ReadError::BufferShort { num_bytes_needed }) => {
                buf.len() + num_bytes_needed
            }
        };
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

type SeqnumMap = SimpleMap<ioctl::UrbHandle, u32>;
type HandleMap = SimpleMap<u32, ioctl::UrbHandle>;
type HandleSeqnumLinker = Arc<Mutex<(SeqnumMap, HandleMap)>>;

struct BorrowSendHandler {
    buf: BytesMut,
    vhci: stub::VhciRemote,
    id: BorrowId,
    cur_seq: Counter,
    prev: ioctl::IocPortStat,
    addr: u8,
    handle_seqnum_map: HandleSeqnumLinker,
}

impl BorrowSendHandler {
    pub fn new(
        vhci: stub::VhciRemote,
        id: BorrowId,
        handle_seqnum_map: HandleSeqnumLinker,
    ) -> Self {
        const BUF_LEN: usize = 16 << 12;
        Self {
            buf: BytesMut::with_capacity(BUF_LEN),
            vhci,
            id,
            cur_seq: Counter::new(0),
            prev: ioctl::IocPortStat::default(),
            addr: 0,
            handle_seqnum_map,
        }
    }
}

impl borrow::SendHandler for BorrowSendHandler {
    #[tracing::instrument(level = "trace", skip_all)]
    fn port_stat(&mut self, next: ioctl::IocPortStat) {
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
            let next_seqnum = self.cur_seq.increment();
            // We pray that we don't run into another handle
            let handle = ioctl::UrbHandle(rand::random());
            {
                let mut guard = self.handle_seqnum_map.lock().unwrap();
                let (seqnums, handles) = guard.deref_mut();
                assert!(seqnums.insert(handle, next_seqnum).is_none());
                assert!(handles.insert(next_seqnum, handle).is_none());
            }
            debug!("({next_seqnum}) port is resetting");

            let header = Header {
                total_frame_len: (size_of::<Header>() / 8) as u16,
                seqnum: next_seqnum,
                command: msg::Command::CmdPort,
                status: msg::Status::Success,
            };

            self.buf.put_slice(header.as_bytes());
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
    }

    #[tracing::instrument(level = "debug", skip_all)]
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
    fn cancel_urb(&mut self, handle: ioctl::UrbHandle) {
        debug!("({}) got cancel {handle:?}", self.id);
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

            self.buf.put_slice(header.as_bytes());
        } else {
            debug!("{handle:?} had already been returned");
        }
    }

    fn process_urb(&mut self, urb: ioctl::IocUrb, handle: ioctl::UrbHandle) -> io::Result<()> {
        debug_assert_eq!(self.addr, urb.address.get());

        let next_seq = self.cur_seq.increment();
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

        // The sender loop flushes our buffer in intervals,
        // so we might have some data from a previous frame.
        // We need to keep track of the head and tail buffers
        // as we make space for our data.

        // Step 1: Reserve enough space for our data.
        {
            let additional = total_frame_len.saturating_sub(self.buf.len());
            self.buf.reserve(additional);
        }

        // Step 2: Split off the head and the tail
        let mut head = self.buf.split();
        let mut frame = {
            // SAFETY: Data will be written to this buf without
            // reading from it.
            unsafe { self.buf.set_len(total_frame_len) };
            self.buf.split_to(total_frame_len)
        };
        let tail = std::mem::replace(&mut self.buf, BytesMut::new());

        // Step 3: Partition our buffer into headers and data sections.
        let (headers, data) = frame.split_at_mut(size_of::<Header>() + size_of::<UrbHeader>());

        // Step 4: If we need to fetch data from VHCI, here's where we do that.
        if needs_fetch {
            let (transfer, iso_data) = if is_out {
                // This is an OUT transfer, therefore we have already reserved
                // the right buffer size for the data. Now we just split the
                // buffer into the transfer and iso data.
                let (transfer, rest) = data.split_at_mut(real_transfer_len);
                let iso_pkts = <[ioctl::IocIsoPacketData]>::mut_from_bytes(rest).unwrap();
                (&mut transfer[..actual_transfer_len], iso_pkts)
            } else {
                // This is an Isochronous IN transfer, therefore we don't need
                // to grab the transfer data since there is none.

                // TEST: We're gonna see if VHCI needs
                // an actual buffer for this part.
                let transfer = &mut [][..];

                // CONTRACT: All of `data` is the isochronous packet buffer.
                let iso_pkts = <[ioctl::IocIsoPacketData]>::mut_from_bytes(data).unwrap();
                (transfer, iso_pkts)
            };

            let borrower_urb = UrbWithIsoData {
                handle,
                header: &urb_header,
                transfer,
                iso_data,
            };

            // It's okay if we exit early without fixing our
            // buffers, since exiting with an error means stopping
            // the entire USB connection.
            self.vhci.fetch_data(borrower_urb)?;
        }

        // if needs_fetch {
        //     let data = &mut buf[header_len..header_len + data_len];
        //     let (transfer, iso_data) = if is_out {
        //         // This is an OUT transfer, therefore we have already reserved
        //         // the right buffer size for the data. Now we just split the
        //         // buffer into the transfer and iso data.
        //         let (transfer, rest) = data.split_at_mut(real_transfer_len);
        //         let iso_data =
        //             <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(rest, packet_count)
        //                 .unwrap();
        //         (&mut transfer[..actual_transfer_len], iso_data)
        //     } else {
        //         // This is an Isochronous IN transfer, therefore we don't need
        //         // to grab the transfer data since there is none.
        //         // So we use the rest of the scratch space as buffer!
        //         // let additional = actual_transfer_len.saturating_sub(self.buf.len());
        //         // self.buf.reserve(additional);

        //         // // SAFETY: Data will never be read.
        //         // unsafe { self.buf.set_len(actual_transfer_len) };
        //         // let transfer = &mut self.buf[..actual_transfer_len];

        //         // TEST: We're gonna see if VHCI needs an actual buffer.
        //         let transfer = &mut [][..];

        //         // CONTRACT: All of `data` is the isochronous packet buffer.
        //         let iso_data =
        //             <[ioctl::IocIsoPacketData]>::mut_from_bytes_with_elems(data, packet_count)
        //                 .unwrap();
        //         (transfer, iso_data)
        //     };

        //     let borrower_urb = UrbWithIsoData {
        //         handle,
        //         header: &urb_header,
        //         transfer,
        //         iso_data,
        //     };

        //     self.vhci.fetch_data(borrower_urb)?;
        // }

        // Step 5: Create and write our headers to their reserved spaces.
        let header = Header {
            total_frame_len: compress_frame_len(total_frame_len),
            seqnum: next_seq,
            command: msg::Command::CmdSubmit,
            status: msg::Status::Success,
        };

        unsafe { headers.as_mut_ptr().cast::<Header>().write(header) };
        unsafe {
            headers
                .as_mut_ptr()
                .byte_add(size_of::<Header>())
                .cast::<UrbHeader>()
                .write(urb_header)
        };

        // Step 6: Rebuild our buffers and store them in our handler.
        head.unsplit(frame);
        head.unsplit(tail);
        self.buf = head;

        // Step 7: Profit??
        Ok(())
    }

    fn is_buf_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn flush_buf(&mut self) -> Bytes {
        self.buf.split().freeze()
    }
}

enum GivebackData {
    In(BytesMut),
    Out(u16),
}

struct OwnedUrbGiveback {
    handle: ioctl::UrbHandle,
    kind: UrbType,
    status: vhci::Status,
    data: GivebackData,
    iso_packets: BytesMut,
    error_count: u16,
}

impl vhci::Urb for OwnedUrbGiveback {
    fn kind(&self) -> ioctl::UrbType {
        self.kind
    }

    fn handle(&self) -> ioctl::UrbHandle {
        self.handle
    }

    fn status(&self) -> vhci::Status {
        self.status
    }

    fn dir(&self) -> vhci::usbfs::Dir {
        match self.data {
            GivebackData::In(_) => Dir::In,
            GivebackData::Out(_) => Dir::Out,
        }
    }

    fn bytes_transferred(&self) -> u16 {
        match self.data {
            GivebackData::In(ref data) => data.len() as u16,
            GivebackData::Out(transferred) => transferred,
        }
    }
}

impl vhci::IsoPacketGivebackMut for OwnedUrbGiveback {
    fn iso_packet_giveback_mut(&mut self) -> &mut [ioctl::IocIsoPacketGiveback] {
        <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes(&mut self.iso_packets).unwrap()
    }

    fn error_count(&self) -> u16 {
        self.error_count
    }
}

impl vhci::TransferMut for OwnedUrbGiveback {
    fn transfer_mut(&mut self) -> &mut [u8] {
        match self.data {
            GivebackData::In(ref mut data) => data,
            GivebackData::Out(_) => &mut [],
        }
    }
}

struct BorrowRecvHandler {
    vhci: stub::VhciRemote,
    id: BorrowId,
    handle_seqnum_map: HandleSeqnumLinker,
}

impl BorrowRecvHandler {
    pub fn new(
        vhci: stub::VhciRemote,
        id: BorrowId,
        handle_seqnum_map: HandleSeqnumLinker,
    ) -> Self {
        Self {
            vhci,
            id,
            handle_seqnum_map,
        }
    }
}

impl borrow::RecvHandler for BorrowRecvHandler {
    async fn urb_reply(&mut self, seqnum: u32, data: Data<UrbFrame>) -> io::Result<()> {
        let handle = {
            let mut guard = self.handle_seqnum_map.lock().unwrap();
            let (seqnums, handles) = guard.deref_mut();
            let handle = handles.remove(&seqnum).unwrap();
            _ = seqnums.remove(&handle).unwrap();
            handle
        };
        let (urb, data) = data.split::<[u8]>();
        let urb = urb.try_read();
        let mut data = data.into_bytes_mut();

        let actual_transfer_len = urb.actual_transfer_len as usize;

        // We might not be expecting data if we sent some to the usb device
        let giveback = OwnedUrbGiveback {
            handle,
            kind: urb.kind,
            status: urb.status,
            data: match urb.is_out() {
                true => GivebackData::Out(urb.actual_transfer_len),
                false => GivebackData::In({
                    let mut data = data.split_to(urb.padded_transfer_len());
                    data.truncate(actual_transfer_len);
                    data
                }),
            },
            iso_packets: data,
            error_count: urb.num_errors,
        };

        self.vhci.giveback_urb(giveback).await?;
        if vhci::Status::Success != urb.status {
            let _guard = warn_span!("urb_reply").entered();
            warn!(
                "({}) {:?} {:?} transfer {seqnum} failed: {:?}",
                self.id,
                urb.kind,
                urb.endpoint.direction(),
                urb.status
            );
        }
        Ok(())
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
    W: AsyncWrite + Unpin + Send + 'static,
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
        let send = compio::runtime::spawn(
            send_loop
                .run(send_handler, cancel_token.clone())
                .in_current_span(),
        );
        let recv_loop = borrow::RecvLoop::new(rx, buf_rx);
        let recv_handler = BorrowRecvHandler::new(vhci.remote(), id, cloned_map);
        let recv = compio::runtime::spawn(
            recv_loop
                .run(recv_handler, cancel_token.clone())
                .in_current_span(),
        );

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

pub(crate) fn open_device(
    dev_id: msg::UsbDeviceId,
) -> rusb::Result<rusb::DeviceHandle<rusb::Context>> {
    rusb::Context::new()
        .and_then(|mut ctx| {
            let span = tracing::trace_span!("libusb");
            // ctx.set_log_level(rusb::LogLevel::Debug);
            ctx.set_log_callback(
                Box::new(move |level, msg| {
                    let _enter = span.enter();
                    match level {
                        rusb::LogLevel::None => (),
                        rusb::LogLevel::Error => error!("{}", msg.trim_end()),
                        rusb::LogLevel::Warning => warn!("{}", msg.trim_end()),
                        rusb::LogLevel::Info => debug!("{}", msg.trim_end()),
                        rusb::LogLevel::Debug => trace!("{}", msg.trim_end()),
                    }
                }),
                rusb::LogCallbackMode::Context,
            );
            ctx.devices()
        })
        // .and_then(|ctx| ctx.devices())
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

#[inline(always)]
fn get_or_alloc_transfer_no_iso(mut cache: RefMut<'_, Vec<InnerTransfer>>) -> InnerTransfer {
    cache.pop().unwrap_or_else(|| InnerTransfer::new(0))
}

#[inline(always)]
fn insert_spare_transfer_no_iso(
    mut cache: RefMut<'_, Vec<InnerTransfer>>,
    transfer: InnerTransfer,
) {
    cache.push(transfer);
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
    let maybe_entry = cache.range_mut(num_iso_packets..).next().map(|(_k, v)| v);
    if let Some(entry) = maybe_entry {
        const EMPTY: OneOrMany<InnerTransfer> = OneOrMany::Many(Vec::new());
        let (transfer, remove_entry) = {
            match std::mem::replace(entry, EMPTY) {
                OneOrMany::One(transfer) => (transfer, true),
                OneOrMany::Many(mut vec) => {
                    let transfer = vec.pop().expect("shouldn't be an empty vec");
                    if vec.is_empty() {
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
        debug_assert!(num_bytes <= DMA_LEN, "requested more bytes than can be held in a single block: {num_bytes} bytes (max: {DMA_LEN} bytes)");
        let queue = &mut self.queue;
        for _ in 0..queue.len() {
            let dma = queue.front_mut().unwrap();
            let additional = num_bytes.saturating_sub(dma.len());
            if dma.try_reclaim(additional) {
                // SAFETY: We won't go over the capacity due to the
                // assertion at the beginning of the function.
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
        //
        // SAFETY: Our device handle is valid and we promise not
        // to use the memory if the USB device is working with it.
        let mut dma = unsafe { self.handle.new_usb_mem(DMA_LEN).unwrap() };

        // SAFETY: We won't go over the capacity due to the
        // assertion at the beginning of the function.
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

#[inline(always)]
fn write_transfer(src: &[u8], dst: &mut [u8]) {
    // let src = <[u64]>::ref_from_bytes(src).unwrap();
    // let dst = <[u64]>::mut_from_bytes(dst).unwrap();
    dst.copy_from_slice(src);
}

#[cold]
#[inline(never)]
fn make_error_header(
    seqnum: u32,
    proto: msg::Status,
    vhci: vhci::Status,
    kind: UrbType,
    endpoint: ioctl::Endpoint,
) -> (Header, io::Error) {
    let _guard = tracing::Span::current().entered();
    error!(
        "({seqnum}) {kind:?} transfer failed on endpoint {}: {vhci:?}",
        endpoint.0
    );
    let header = Header {
        total_frame_len: compress_frame_len(size_of::<Header>()),
        command: msg::Command::RetSubmit,
        status: proto,
        seqnum,
    };
    let errno = vhci.to_errno_raw(UrbType::Iso == kind);
    let err = io::Error::from_raw_os_error(-errno);
    (header, err)
}

#[derive(Debug)]
pub struct Seq<T> {
    pub seqnum: u32,
    pub data: T,
}

#[inline]
fn padding(actual_transfer_len: u16) -> &'static [u8] {
    static PADDING: [u8; 7] = [0; 7];
    let actual_transfer_len = usize::from(actual_transfer_len);
    let padded_len = align_to_usize(actual_transfer_len) - actual_transfer_len;
    &PADDING[..padded_len]
}

#[inline]
const fn compress_frame_len(len: usize) -> u16 {
    (len / size_of::<u64>()) as u16
}

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
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn lend(self, cancel_token: CancellationToken) -> Result<()> {
        let Self {
            mut tx,
            rx,
            mut buf_rx,
            device,
            id: dev_id,
        } = self;

        enum Event {
            RecvFrame2(io::Result<Recv>),
            FlushBuf,
            CompletedIso(Seq<Iso>),
            CompletedInt(Seq<Int>),
            CompletedCtrl(Seq<Ctrl>),
            CompletedBulk(Seq<Bulk>),
            Cancelled,
        }

        let device = Arc::new(device);
        let event_handler = CancellationToken::new();
        let runtime = event_handler.clone();
        let ctx = device.context().clone();
        let libusb_handle = compio::runtime::spawn_blocking(move || {
            let _guard = warn_span!("libusb_event_handler").entered();
            while !runtime.is_cancelled() {
                if let Err(err) = ctx.handle_events(Some(Duration::from_secs(5))) {
                    warn! { %err };
                }
            }
        });

        // ---- Bookkeeping ----
        let mut cancel_tokens: IntMap<u32, CancellationToken> =
            IntMap::with_capacity_and_hasher(1024, Default::default());
        let claimed_interfaces: Arc<Mutex<IntSet<u8>>> = Arc::new(Mutex::new(
            IntSet::with_capacity_and_hasher(16, Default::default()),
        ));

        // ---- Cache ----
        const BUF_LEN: usize = 16 << 10;
        buf_rx.reserve(BUF_LEN);
        let mut buf_tx = BytesMut::with_capacity(BUF_LEN);
        let mut scratch_dma = DmaAllocator::with_capacity(5, Arc::clone(&device));
        let iso_transfers: Rc<RefCell<BTreeMap<u16, OneOrMany<InnerTransfer>>>> = Rc::default();
        let small_transfers: Rc<RefCell<Vec<InnerTransfer>>> = Rc::default();

        // ---- Event Receivers ----
        const TICK: Duration = Duration::from_micros(50);
        let (blocking_tx, blocking_rx) = mpsc::channel::<Seq<vhci::Status>>(0);
        let mut blocking_rx = blocking_rx.into_stream();
        let (iso_tx, iso_rx) = mpsc::channel::<Seq<Iso>>(512);
        let mut iso_rx = iso_rx.into_stream();
        let (int_tx, int_rx) = mpsc::channel::<Seq<Int>>(0);
        let mut int_rx = int_rx.into_stream();
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<Seq<Ctrl>>(0);
        let mut ctrl_rx = ctrl_rx.into_stream();
        let (bulk_tx, bulk_rx) = mpsc::channel::<Seq<Bulk>>(16);
        let mut bulk_rx = bulk_rx.into_stream();

        let blocking = (&mut blocking_rx).map(|Seq { seqnum, data }| {
            Event::CompletedCtrl(Seq {
                seqnum,
                data: Ctrl {
                    res: ResultData::Out {
                        bytes_transferred: 0,
                    },
                    status: data,
                },
            })
        });
        let iso = (&mut iso_rx).map(Event::CompletedIso);
        let int = (&mut int_rx).map(Event::CompletedInt);
        let ctrl = (&mut ctrl_rx).map(Event::CompletedCtrl);
        let bulk = (&mut bulk_rx).map(Event::CompletedBulk);
        let mut timer = pin!(sleep(TICK));
        let cancel = stream::once_future(async move {
            cancel_token.cancelled_owned().await;
            Event::Cancelled
        });
        let frame = stream::unfold((rx, buf_rx), |(mut rx, mut buf)| async {
            let result = recv_frame(&mut rx, &mut buf).await.transpose()?;
            Some((Event::RecvFrame2(result), (rx, buf)))
        });

        let mut main_events = pin!((cancel, frame, iso, int, ctrl, blocking, bulk).merge());

        // ---- Stats ----
        let mut max_active_transfers = 0;
        let mut num_active_transfers = 0;
        let mut max_completed_transfers_before_flush = 0;
        let mut num_transfers_completed = 0;
        let mut min_setup_time = Duration::MAX;
        let mut max_setup_time = Duration::ZERO;
        const THRESHOLD: usize = 64;
        let mut last_few_setup_times =
            metrics::RollingAvg::<THRESHOLD, metrics::Duration>::preallocated();

        let mut main = main_events.next();

        let result: std::result::Result<(), (Header, io::Error)> = loop {
            let event = if buf_tx.is_empty() {
                pin!(&mut main).await
            } else {
                let timer = async {
                    timer.as_mut().await;
                    Some(Event::FlushBuf)
                };
                (pin!(&mut main), timer).race().await
            };

            match event {
                Some(Event::FlushBuf) => {
                    max_completed_transfers_before_flush = std::cmp::max(
                        max_completed_transfers_before_flush,
                        num_transfers_completed,
                    );
                    num_transfers_completed = 0;
                    let bytes = buf_tx.split().freeze();
                    tx.write_all(bytes).await.0.unwrap();
                    timer.set(sleep(TICK));
                }
                Some(Event::RecvFrame2(Ok(Recv::Urb((Header { seqnum, .. }, urb_frame))))) => {
                    num_active_transfers += 1;
                    max_active_transfers =
                        std::cmp::max(max_active_transfers, num_active_transfers);
                    let start = std::time::Instant::now();
                    let cancel = CancellationToken::new();
                    cancel_tokens.insert(seqnum, cancel.clone());

                    let (urb, data) = urb_frame.split::<[u8]>();
                    let urb = urb.try_read();
                    let mut data = data.into_bytes_mut();
                    match urb.kind {
                        UrbType::Iso => {
                            let padded_transfer_len = urb.padded_transfer_len();
                            let is_out = urb.is_out();

                            let (transfer_buf, mut raw_iso_buf) = if is_out {
                                let transfer_data = data.split_to(padded_transfer_len);
                                let mut dma = scratch_dma.reserve(padded_transfer_len);
                                write_transfer(&transfer_data, &mut dma);
                                let transfer_buf = dma.split_to(urb.actual_transfer_len as usize);
                                (transfer_buf, data)
                            } else {
                                let mut transfer_buf = scratch_dma.reserve(padded_transfer_len);
                                let transfer_buf =
                                    transfer_buf.split_to(urb.actual_transfer_len as usize);
                                (transfer_buf, data)
                            };

                            #[repr(transparent)]
                            struct Pkt(ioctl::IocIsoPacketData);
                            impl IsoPacket for Pkt {
                                fn len(&self) -> u32 {
                                    self.0.packet_length
                                }
                            }

                            struct Iter<'a> {
                                pkts: std::slice::Iter<'a, ioctl::IocIsoPacketData>,
                            }
                            impl Iterator for Iter<'_> {
                                type Item = Pkt;
                                fn next(&mut self) -> Option<Self::Item> {
                                    self.pkts.next().map(|pkt| Pkt(*pkt))
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

                            let num_iso_pkts = urb.iso_packet_count as usize;
                            let iso_pkts = <[ioctl::IocIsoPacketData]>::ref_from_bytes_with_elems(
                                &raw_iso_buf[..],
                                num_iso_pkts,
                            )
                            .unwrap();

                            let transfer = get_or_alloc_transfer(
                                iso_transfers.borrow_mut(),
                                urb.iso_packet_count,
                            );
                            let endpoint = urb.endpoint;
                            let interval = urb.interval;
                            let transfer = unsafe {
                                transfer.into_iso(
                                    &device,
                                    endpoint.0,
                                    transfer_buf,
                                    Iter {
                                        pkts: iso_pkts.iter(),
                                    },
                                )
                            };

                            let cache = iso_transfers.clone();
                            let iso_tx = iso_tx.clone();
                            compio::runtime::spawn(async move {
                                let result = transfer.submit(&cancel).await;
                                let status =
                                    convert_libusb_to_vhci(result, UrbType::Iso, seqnum, dev_id);

                                let our_pkts =
                                    <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                                        &mut raw_iso_buf[..],
                                        num_iso_pkts,
                                    )
                                    .unwrap();
                                let their_pkts = transfer
                                    .iso_packets()
                                    .expect("why wouldn't a transfer be done by this point?");

                                let mut num_errors = 0;
                                for (our_pkt, libusb_pkt) in our_pkts.iter_mut().zip(their_pkts) {
                                    our_pkt.packet_actual = libusb_pkt.actual_len();
                                    our_pkt.status = vhci_from_transfer_status(libusb_pkt.status())
                                        .to_errno_raw(true);
                                    if our_pkt.status != 0 {
                                        num_errors += 1;
                                    }
                                }
                                let (transfer, mut buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer(cache.borrow_mut(), transfer);

                                // SAFETY: Isochronous transfer requires that the full
                                // buffer be sent back to the caller.
                                unsafe { buf.set_len(buf.capacity()) };

                                let msg = Seq {
                                    seqnum,
                                    data: Iso {
                                        res: match is_out {
                                            true => ResultData::Out {
                                                bytes_transferred: buf.len(),
                                            },
                                            false => ResultData::In(buf),
                                        },
                                        endpoint,
                                        interval,
                                        raw_iso_buf,
                                        num_errors,
                                        num_iso_packets: num_iso_pkts as u16,
                                        status,
                                    },
                                };
                                _ = iso_tx.into_send_async(msg).await;
                            })
                            .detach();
                            let elapsed = start.elapsed();
                            last_few_setup_times.push(elapsed);
                            min_setup_time = std::cmp::min(min_setup_time, elapsed);
                            max_setup_time = std::cmp::max(max_setup_time, elapsed);
                        }
                        UrbType::Int => {
                            let mut buf = {
                                let needed = urb.padded_transfer_len();
                                scratch_dma.reserve(needed)
                            };

                            let is_out = urb.is_out();
                            if is_out {
                                write_transfer(&data, &mut buf);
                            }

                            let buf = buf.split_to(urb.actual_transfer_len as usize);
                            let transfer =
                                get_or_alloc_transfer_no_iso(small_transfers.borrow_mut());
                            let endpoint = urb.endpoint;
                            let interval = urb.interval;

                            // SAFETY: Transfer buffer has a capacity of actual_transfer_len.
                            let transfer = unsafe { transfer.into_int(&device, endpoint.0, buf) };

                            let cache = small_transfers.clone();
                            let int_tx = int_tx.clone();
                            compio::runtime::spawn(async move {
                                let result = transfer.submit(&cancel).await;
                                let status =
                                    convert_libusb_to_vhci(result, UrbType::Int, seqnum, dev_id);

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer_no_iso(cache.borrow_mut(), transfer);

                                let msg = Seq {
                                    seqnum,
                                    data: Int {
                                        res: match is_out {
                                            true => ResultData::Out {
                                                bytes_transferred: buf.len(),
                                            },
                                            false => ResultData::In(buf),
                                        },
                                        endpoint,
                                        interval,
                                        status,
                                    },
                                };
                                _ = int_tx.into_send_async(msg).await;
                            })
                            .detach();
                            let elapsed = start.elapsed();
                            last_few_setup_times.push(elapsed);
                            min_setup_time = std::cmp::min(min_setup_time, elapsed);
                            max_setup_time = std::cmp::max(max_setup_time, elapsed);
                        }
                        UrbType::Ctrl => match lend::CtrlKind::parse(urb.ctrl_packet) {
                            lend::CtrlKind::Blocking(lend::blocking::CtrlReq::SetInterface {
                                setting,
                                interface,
                            }) => {
                                let blocking_tx = blocking_tx.clone();
                                let device = Arc::clone(&device);
                                compio::runtime::spawn(async move {
                                    let msg = device
                                        .set_alt_setting_async(seqnum, interface, setting)
                                        .instrument(trace_span!("transfer"))
                                        .await;
                                    _ = blocking_tx.into_send_async(msg).await;
                                })
                                .detach();
                                let elapsed = start.elapsed();
                                last_few_setup_times.push(elapsed);
                                min_setup_time = std::cmp::min(min_setup_time, elapsed);
                                max_setup_time = std::cmp::max(max_setup_time, elapsed);
                            }
                            lend::CtrlKind::Blocking(lend::blocking::CtrlReq::SetConfig {
                                desired,
                            }) => {
                                let interfaces = Arc::clone(&claimed_interfaces);
                                let blocking_tx = blocking_tx.clone();
                                let device = Arc::clone(&device);
                                compio::runtime::spawn(async move {
                                    let msg = device
                                        .set_config_async(seqnum, desired, interfaces)
                                        .instrument(trace_span!("transfer"))
                                        .await;
                                    _ = blocking_tx.into_send_async(msg).await;
                                })
                                .detach();
                                let elapsed = start.elapsed();
                                last_few_setup_times.push(elapsed);
                                min_setup_time = std::cmp::min(min_setup_time, elapsed);
                                max_setup_time = std::cmp::max(max_setup_time, elapsed);
                            }
                            lend::CtrlKind::Blocking(lend::blocking::CtrlReq::ClearStall {
                                endpoint,
                            }) => {
                                let blocking_tx = blocking_tx.clone();
                                let device = Arc::clone(&device);
                                compio::runtime::spawn(async move {
                                    let msg = device
                                        .clear_stall_async(seqnum, endpoint)
                                        .instrument(trace_span!("transfer"))
                                        .await;
                                    _ = blocking_tx.into_send_async(msg).await;
                                })
                                .detach();
                                let elapsed = start.elapsed();
                                last_few_setup_times.push(elapsed);
                                min_setup_time = std::cmp::min(min_setup_time, elapsed);
                                max_setup_time = std::cmp::max(max_setup_time, elapsed);
                            }
                            lend::CtrlKind::Async(ctrl_pkt) => {
                                let is_get_status =
                                    Request::STANDARD_DEVICE_GET_STATUS == ctrl_pkt.req();
                                let size_of_pkt = size_of::<ioctl::IocSetupPacket>();
                                let actual_transfer_len = urb.actual_transfer_len as usize;
                                let mut buf = {
                                    let w_length = ctrl_pkt.length() as usize;
                                    debug_assert_eq!(w_length, actual_transfer_len);
                                    let needed = size_of_pkt + urb.padded_transfer_len();
                                    scratch_dma.reserve(needed)
                                };

                                let (setup_space, rest) =
                                    ioctl::IocSetupPacket::mut_from_prefix(&mut buf).unwrap();

                                *setup_space = ctrl_pkt;
                                let is_out = Dir::Out == ctrl_pkt.req().dir();
                                if is_out {
                                    write_transfer(&data, rest);
                                }

                                let buf = buf.split_to(actual_transfer_len + size_of_pkt);
                                let transfer =
                                    get_or_alloc_transfer_no_iso(small_transfers.borrow_mut());

                                // SAFETY: Transfer buffer is exactly the length needed for
                                // a control transfer, and matches w_length + 8.
                                let transfer = unsafe {
                                    transfer.into_ctrl(&device, buf, Duration::from_millis(900))
                                };

                                let cache = small_transfers.clone();
                                let ctrl_tx = ctrl_tx.clone();
                                compio::runtime::spawn(async move {
                                    let result = transfer.submit(&cancel).await;
                                    let status =
                                        convert_libusb_to_vhci(result, urb.kind, seqnum, dev_id);

                                    let mut buf = {
                                        let (transfer, mut buf) = transfer.into_parts().unwrap();
                                        insert_spare_transfer_no_iso(cache.borrow_mut(), transfer);
                                        buf.split_off(size_of_pkt)
                                    };

                                    if is_get_status {
                                        // Indicate that our fake USB device is self powered.
                                        buf[0] = 0x01;
                                    }

                                    let msg = Seq {
                                        seqnum,
                                        data: Ctrl {
                                            res: match is_out {
                                                true => ResultData::Out {
                                                    bytes_transferred: buf.len(),
                                                },
                                                false => ResultData::In(buf),
                                            },
                                            status,
                                        },
                                    };
                                    _ = ctrl_tx.into_send_async(msg).await;
                                })
                                .detach();
                                let elapsed = start.elapsed();
                                last_few_setup_times.push(elapsed);
                                min_setup_time = std::cmp::min(min_setup_time, elapsed);
                                max_setup_time = std::cmp::max(max_setup_time, elapsed);
                            }
                        },
                        UrbType::Bulk => {
                            let padded_transfer_len = urb.padded_transfer_len();
                            let mut buf = scratch_dma.reserve(padded_transfer_len);

                            let is_out = urb.is_out();
                            if is_out {
                                write_transfer(&data, &mut buf);
                            }

                            let buf = buf.split_to(urb.actual_transfer_len as usize);
                            let transfer =
                                get_or_alloc_transfer_no_iso(small_transfers.borrow_mut());
                            let endpoint = urb.endpoint;

                            // SAFETY: Transfer buffer has a capacity of actual_transfer_len.
                            let transfer = unsafe {
                                transfer.into_bulk(&device, endpoint.0, TransferFlags::NONE, buf)
                            };

                            let cache = small_transfers.clone();
                            let bulk_tx = bulk_tx.clone();
                            compio::runtime::spawn(async move {
                                let result = transfer.submit(&cancel).await;
                                let status =
                                    convert_libusb_to_vhci(result, UrbType::Bulk, seqnum, dev_id);

                                let (transfer, buf) = transfer.into_parts().unwrap();
                                insert_spare_transfer_no_iso(cache.borrow_mut(), transfer);

                                let msg = Seq {
                                    seqnum,
                                    data: Bulk {
                                        res: match is_out {
                                            true => ResultData::Out {
                                                bytes_transferred: buf.len(),
                                            },
                                            false => ResultData::In(buf),
                                        },
                                        endpoint,
                                        status,
                                    },
                                };
                                _ = bulk_tx.into_send_async(msg).await;
                            })
                            .detach();
                            let elapsed = start.elapsed();
                            last_few_setup_times.push(elapsed);
                            min_setup_time = std::cmp::min(min_setup_time, elapsed);
                            max_setup_time = std::cmp::max(max_setup_time, elapsed);
                        }
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedCtrl(Seq {
                    seqnum,
                    data:
                        Ctrl {
                            res: ResultData::In(buf),
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let actual_transfer_len = buf.len() as u16;
                        let padding = padding(actual_transfer_len);
                        let urb_header = UrbHeader {
                            actual_transfer_len,
                            iso_packet_count: 0,
                            endpoint: ioctl::Endpoint(0x80),
                            kind: UrbType::Ctrl,
                            interval: 0,
                            status: vhci_status,
                            flags: 0,
                            num_errors: 0,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>()
                                + size_of::<UrbHeader>()
                                + align_to_usize(buf.len());
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };

                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                        buf_tx.put_slice(&buf);
                        buf_tx.put_slice(padding);
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Ctrl,
                            ioctl::Endpoint(0x80),
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedCtrl(Seq {
                    seqnum,
                    data:
                        Ctrl {
                            res: ResultData::Out { bytes_transferred },
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let urb_header = UrbHeader {
                            actual_transfer_len: bytes_transferred as u16,
                            iso_packet_count: 0,
                            endpoint: ioctl::Endpoint(0),
                            kind: UrbType::Ctrl,
                            interval: 0,
                            status: vhci_status,
                            flags: 0,
                            num_errors: 0,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>() + size_of::<UrbHeader>();
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };
                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Ctrl,
                            ioctl::Endpoint(0),
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedInt(Seq {
                    seqnum,
                    data:
                        Int {
                            res: ResultData::In(buf),
                            endpoint,
                            interval,
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let actual_transfer_len = buf.len() as u16;
                        let padding = padding(actual_transfer_len);
                        let urb_header = UrbHeader {
                            actual_transfer_len,
                            iso_packet_count: 0,
                            endpoint,
                            kind: UrbType::Int,
                            interval,
                            status: vhci_status,
                            flags: 0,
                            num_errors: 0,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>()
                                + size_of::<UrbHeader>()
                                + align_to_usize(buf.len());
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };

                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                        buf_tx.put_slice(&buf);
                        buf_tx.put_slice(padding);
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Int,
                            endpoint,
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedInt(Seq {
                    seqnum,
                    data:
                        Int {
                            res: ResultData::Out { bytes_transferred },
                            endpoint,
                            interval,
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let urb_header = UrbHeader {
                            actual_transfer_len: bytes_transferred as u16,
                            iso_packet_count: 0,
                            endpoint,
                            kind: UrbType::Int,
                            interval,
                            status: vhci_status,
                            flags: 0,
                            num_errors: 0,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>() + size_of::<UrbHeader>();
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };
                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Int,
                            endpoint,
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedBulk(Seq {
                    seqnum,
                    data:
                        Bulk {
                            res: ResultData::In(buf),
                            endpoint,
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let actual_transfer_len = buf.len() as u16;
                        let padding = padding(actual_transfer_len);
                        let urb_header = UrbHeader {
                            actual_transfer_len,
                            iso_packet_count: 0,
                            endpoint,
                            kind: UrbType::Bulk,
                            interval: 0,
                            status: vhci_status,
                            flags: 0,
                            num_errors: 0,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>()
                                + size_of::<UrbHeader>()
                                + align_to_usize(buf.len());
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };

                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                        buf_tx.put_slice(&buf);
                        buf_tx.put_slice(padding);
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Bulk,
                            endpoint,
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedBulk(Seq {
                    seqnum,
                    data:
                        Bulk {
                            res: ResultData::Out { bytes_transferred },
                            endpoint,
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let urb_header = UrbHeader {
                            actual_transfer_len: bytes_transferred as u16,
                            iso_packet_count: 0,
                            endpoint,
                            kind: UrbType::Bulk,
                            interval: 0,
                            status: vhci_status,
                            flags: 0,
                            num_errors: 0,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>() + size_of::<UrbHeader>();
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };
                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Bulk,
                            endpoint,
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedIso(Seq {
                    seqnum,
                    data:
                        Iso {
                            res: ResultData::In(buf),
                            endpoint,
                            interval,
                            raw_iso_buf,
                            num_errors,
                            num_iso_packets,
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let actual_transfer_len = buf.len() as u16;
                        let padding = padding(actual_transfer_len);
                        let urb_header = UrbHeader {
                            actual_transfer_len,
                            iso_packet_count: num_iso_packets,
                            endpoint,
                            kind: UrbType::Iso,
                            interval,
                            status: vhci_status,
                            flags: 0,
                            num_errors,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len = size_of::<Header>()
                                + size_of::<UrbHeader>()
                                + align_to_usize(buf.len())
                                + raw_iso_buf.len();
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };

                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                        buf_tx.put_slice(&buf);
                        buf_tx.put_slice(padding);
                        buf_tx.put_slice(&raw_iso_buf);
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Iso,
                            endpoint,
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::CompletedIso(Seq {
                    seqnum,
                    data:
                        Iso {
                            res: ResultData::Out { bytes_transferred },
                            endpoint,
                            interval,
                            raw_iso_buf,
                            num_errors,
                            num_iso_packets,
                            status: vhci_status,
                        },
                })) => {
                    num_transfers_completed += 1;
                    num_active_transfers -= 1;
                    cancel_tokens.remove(&seqnum);
                    let status = msg::Status::from(vhci_status);

                    if msg::Status::Success == status {
                        let urb_header = UrbHeader {
                            actual_transfer_len: bytes_transferred as u16,
                            iso_packet_count: num_iso_packets,
                            endpoint,
                            kind: UrbType::Iso,
                            interval,
                            status: vhci_status,
                            flags: 0,
                            num_errors,
                            ctrl_packet: Default::default(),
                        };
                        let total_frame_len = {
                            let len =
                                size_of::<Header>() + size_of::<UrbHeader>() + raw_iso_buf.len();
                            compress_frame_len(len)
                        };
                        let header = Header {
                            total_frame_len,
                            command: msg::Command::RetSubmit,
                            status,
                            seqnum,
                        };
                        buf_tx.put_slice(header.as_bytes());
                        buf_tx.put_slice(urb_header.as_bytes());
                        buf_tx.put_slice(&raw_iso_buf);
                    } else {
                        break Err(make_error_header(
                            seqnum,
                            status,
                            vhci_status,
                            UrbType::Iso,
                            endpoint,
                        ));
                    }
                    main = main_events.next();
                }
                Some(Event::RecvFrame2(Ok(Recv::PortReset(header)))) => {
                    trace!("({}) got port reset", header.seqnum);

                    let handle = Arc::clone(&device);
                    let span = tracing::Span::current();
                    let port_reset = compio::runtime::spawn_blocking(move || {
                        let _guard = span.entered();
                        port_reset(header.seqnum, dev_id, &handle)
                    });

                    let status = port_reset.await.unwrap();

                    let header = Header {
                        command: msg::Command::RetPort,
                        status,
                        ..header
                    };

                    if msg::Status::Success == header.status {
                        buf_tx.put_u64_le(transmute!(header));
                    } else {
                        break Err((header, io::ErrorKind::ResourceBusy.into()));
                    }
                    main = main_events.next();
                }
                Some(Event::RecvFrame2(Ok(Recv::Unlink(header)))) => {
                    if let Some(transfer) = cancel_tokens.remove(&header.seqnum) {
                        transfer.cancel();
                    }
                    main = main_events.next();
                }
                Some(Event::Cancelled) | None => break Ok(()),
                Some(Event::RecvFrame2(Err(_err))) => todo!(),
            }
        };

        let result = if let Err((header, err)) = result {
            tx.write_u64_le(transmute!(header)).await?;
            Err(Error::Io(err))
        } else {
            Ok(())
        };

        // ---- Cleanup ----
        info!(
            "shutting down ({:03}/{:03})",
            dev_id.bus_number, dev_id.device_addr
        );
        cancel_tokens
            .into_values()
            .for_each(|transfer| transfer.cancel());
        drop(blocking_tx);
        drop(iso_tx);
        drop(int_tx);
        drop(ctrl_tx);
        drop(bulk_tx);

        trace!(
            "waiting for all transfers to complete ({:03}/{:03})",
            dev_id.bus_number,
            dev_id.device_addr
        );
        while blocking_rx.next().await.is_some() {}
        while iso_rx.next().await.is_some() {}
        while int_rx.next().await.is_some() {}
        while ctrl_rx.next().await.is_some() {}
        while bulk_rx.next().await.is_some() {}
        event_handler.cancel();
        drop(device);
        trace!("joining event handler loop");
        libusb_handle.await.unwrap();

        info!("==== STATS ====");
        info!("Max active transfers: {max_active_transfers}");
        info!("Max completed transfers before flush: {max_completed_transfers_before_flush}");
        info!("Number of DMA regions in use: {}", scratch_dma.queue.len());
        info!("Min time to setup transfer: {min_setup_time:?}");
        info!("Max time to setup transfer: {max_setup_time:?}");
        info!(
            "Avg setup time for last {THRESHOLD} transfers: {:?}",
            Duration::from(last_few_setup_times.mean().unwrap_or_default())
        );

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
    W: AsyncWrite + Unpin,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn send_device_list<'a, I, T>(self, iter: impl Fn() -> io::Result<I>) -> Result<()>
    where
        I: Iterator<Item = T>,
        T: msg::SendUsbDeviceInfo,
    {
        let mut tx = self.tx;

        trace!("getting available devices to send to peer");

        let mut buf = BytesMut::new();
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
                return Err(err.into());
            }
        };

        for usb in devices {
            let mut device = usb
                .get()
                .as_bytes()
                .chain(usb.interfaces_with_padding().as_bytes());
            buf.put(&mut device);
        }
        tx.write_all(buf).await.0?;

        trace!("sent all devices to peer");
        Ok(())
    }
}

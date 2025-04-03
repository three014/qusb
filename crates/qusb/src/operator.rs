use std::{
    cell::RefCell,
    collections::BTreeMap,
    io,
    ops::DerefMut,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use compio_io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use futures_concurrency::future::Join;
use lend::{
    Bulk, ClearStall, Ctrl, Int, Iso, ResultData, SetConfig, SetInterface, blocking::BlockingOps,
};
use nohash_hasher::IntMap;
use proto::{
    data::{Data, ReadError, Ring},
    msg::{self, Header, QusbFrame, UrbFrame, UrbHeader},
};
use rusb::UsbContext;
use rusb_async::{
    CancellationToken as CancelTransferToken, IsoPacket, LibusbTransfer2, Transfer2, TransferFlags,
    TransferStatus,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, error, info, trace, warn, warn_span};
use vhci::{
    DataRate, PortChange, PortFlag, PortStatus,
    ioctl::{self, UrbType},
    usbfs::{Dir, Request},
};
use zerocopy::{FromBytes, IntoBytes, transmute};

use crate::{
    Result, UrbWithIsoData,
    stub::{self, RegisterPort},
    utils::{
        CloseStream, Counter, SimpleMap, align_to_usize,
        alloc::BytesAllocator,
        metrics::{self, Max, Min, RollingAvg},
        task,
    },
};

mod borrow;
mod lend;

enum Recv {
    Urb((Header, Option<Data<UrbFrame>>)),
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
            Err(ReadError::CorruptedData) => {
                #[cold]
                #[inline(never)]
                fn report_corrupted_data() -> io::Result<Option<Recv>> {
                    Err(io::Error::other("corrupted data from peer"))?
                }
                return report_corrupted_data();
            }
            Err(ReadError::BufferShort { num_bytes_needed }) => buf.len() + num_bytes_needed,
        };
    };

    let frame_ref = frame.get();
    match frame_ref.header.command {
        msg::Command::CmdUnlink => Ok(Some(Recv::Unlink(frame_ref.header))),
        msg::Command::CmdPort | msg::Command::RetPort => {
            Ok(Some(Recv::PortReset(frame_ref.header)))
        }
        msg::Command::RetSubmit | msg::Command::CmdSubmit => {
            let header = frame_ref.header;
            let urb = frame.split_data::<UrbFrame>();
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
            .field("remote", &self.remote_dev)
            .field("local", &self.local_port.get())
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
    #[inline]
    fn kind(&self) -> ioctl::UrbType {
        self.ioctl_urb.typ
    }

    #[inline]
    fn handle(&self) -> ioctl::UrbHandle {
        self.handle
    }

    #[inline]
    fn status(&self) -> vhci::Status {
        vhci::Status::Success
    }

    #[inline]
    fn dir(&self) -> vhci::usbfs::Dir {
        self.ioctl_urb.endpoint.direction()
    }

    #[inline]
    fn bytes_transferred(&self) -> u16 {
        self.ioctl_urb.buffer_length as u16
    }
}

impl vhci::TransferMut for EmptyUrb {
    #[inline]
    fn transfer_mut(&mut self) -> &mut [u8] {
        &mut []
    }
}

impl vhci::IsoPacketGivebackMut for EmptyUrb {
    #[inline]
    fn iso_packet_giveback_mut(&mut self) -> &mut [ioctl::IocIsoPacketGiveback] {
        &mut []
    }

    #[inline]
    fn error_count(&self) -> u16 {
        0
    }
}

type SeqnumMap = SimpleMap<ioctl::UrbHandle, u32>;
type HandleMap = IntMap<u32, ioctl::UrbHandle>;
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
    #[inline]
    pub fn new(
        vhci: stub::VhciRemote,
        id: BorrowId,
        handle_seqnum_map: HandleSeqnumLinker,
    ) -> Self {
        const BUF_LEN: usize = 16 << 14;
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
        let address_invalidated = next.change().contains(PortChange::CONNECTION);
        let reset_successful = next.change().contains(PortChange::RESET)
            && next.status().complement().contains(PortStatus::RESET)
            && next.status().contains(PortStatus::ENABLE);
        let power_off = self.prev.status().contains(PortStatus::POWER)
            && next.status().complement().contains(PortStatus::POWER);
        let send_port_reset = self.prev.status().complement().contains(PortStatus::RESET)
            && next
                .status()
                .contains(PortStatus::RESET | PortStatus::CONNECTION);
        let resuming = self.prev.flags().complement().contains(PortFlag::RESUMING)
            && next.flags().contains(PortFlag::RESUMING)
            && next.status().contains(PortStatus::CONNECTION);

        if address_invalidated {
            debug!("CONNECTION state changed -> invalidating address");
            self.addr = 0xff;
        } else if reset_successful {
            debug!("RESET successful -> use default address");
            self.addr = 0;
        } else if power_off {
            debug!("port is powered off");
        } else if send_port_reset {
            // We pray that we don't run into another handle
            let handle = ioctl::UrbHandle(rand::random());
            let next_seqnum = self.cur_seq.increment();
            {
                let mut guard = self.handle_seqnum_map.lock().unwrap();
                let (seqnums, handles) = guard.deref_mut();
                assert!(seqnums.insert(handle, next_seqnum).is_none());
                assert!(handles.insert(next_seqnum, handle).is_none());
            }
            debug!("({next_seqnum}) port is resetting");

            let header = Header {
                total_frame_len: compress_frame_len(size_of::<Header>()),
                seqnum: next_seqnum,
                command: msg::Command::CmdPort,
                status: msg::Status::Success,
            };

            self.buf.put_slice(header.as_bytes());
        } else if resuming {
            debug!("port is resuming -> completing resume");
        } else {
            debug!(
                "{:?} {:?} {:?} {:?}",
                next.status(),
                next.change(),
                next.index(),
                next.flags()
            );
        }

        self.prev = next;
    }

    fn set_address(
        &mut self,
        urb: ioctl::IocUrb,
        handle: ioctl::UrbHandle,
    ) -> impl Future<Output = io::Result<()>> + 'static {
        let _enter = Span::current().entered();
        self.addr = urb.setup_packet.value() as u8;
        debug!("({}) set local dev address to {:03}", self.id, self.addr);
        let mut remote = self.vhci.clone();
        let urb = EmptyUrb {
            handle,
            ioctl_urb: urb,
        };

        remote.giveback_urb(urb)
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
            _ = handles.insert(next_seq, handle);
            _ = seqnums.insert(handle, next_seq);
            // debug_assert!(handles.insert(next_seq, handle).is_none());
            // debug_assert!(seqnums.insert(handle, next_seq).is_none());
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
        self.buf.reserve(total_frame_len);

        // Step 2: Split off the head and frame. (The tail
        // stays attached until later when we recombine
        // all the buffers)
        let mut head = self.buf.split();
        let mut frame = {
            // SAFETY: Data will be written to this buf without
            // reading from it.
            unsafe { self.buf.set_len(total_frame_len) };
            self.buf.split_to(total_frame_len)
        };

        // Step 3: Partition our buffer into headers and data sections.
        //
        // SAFETY: We just reserved `total_frame_len` bytes, which is always
        // at least as long as `header_len`.
        let (headers, data) = unsafe { frame.split_at_mut_unchecked(header_len) };
        // let (headers, data) = frame.split_at_mut(header_len);

        // Step 4: If we need to fetch data from VHCI, here's where we do that.
        {
            // SAFETY: We just reserved `total_frame_len` bytes, which gets
            // calculated from `data_len`, which is calculated from `real_transfer_len`.
            // Therefore, this slice is at least as long as `real_transfer_len`.
            let (transfer, iso_raw) = unsafe { data.split_at_mut_unchecked(real_transfer_len) };

            // SAFETY: `actual_transfer_len <= padded_transfer_len`, therefore
            // `actual_transfer_len * 0 == padded_transfer_len * 0` and
            // `actual_transfer_len * 1 <= padded_transfer_len * 1`.
            let transfer =
                unsafe { transfer.get_unchecked_mut(..actual_transfer_len * is_out as usize) };

            let iso_data = <[ioctl::IocIsoPacketData]>::mut_from_bytes(iso_raw).unwrap();
            let mut borrower_urb = UrbWithIsoData {
                handle,
                header: &urb_header,
                transfer,
                iso_data,
            };

            if needs_fetch {
                // It's okay if we exit early without fixing our
                // buffers, since exiting with an error means stopping
                // the entire USB connection.
                self.vhci.fetch_data(&mut borrower_urb)?;
            }
        }

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

        // `BytesMut::unsplit` attaches the buffer at the end
        // of the current buffer so we need to swap our tail with
        // the head so the head can reabsorb the tail.
        let tail = std::mem::replace(&mut self.buf, head);
        self.buf.unsplit(tail);

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

#[derive(Debug)]
enum GivebackData {
    In(BytesMut),
    Out(u16),
}

#[derive(Debug)]
struct OwnedUrbGiveback {
    handle: ioctl::UrbHandle,
    kind: UrbType,
    status: vhci::Status,
    data: GivebackData,
    iso_packets: BytesMut,
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
        <Self as vhci::IsoPacketGiveback>::error_count(&self)
    }
}

impl vhci::IsoPacketGiveback for OwnedUrbGiveback {
    fn iso_packet_giveback(&self) -> &[ioctl::IocIsoPacketGiveback] {
        <[ioctl::IocIsoPacketGiveback]>::ref_from_bytes(&self.iso_packets).unwrap()
    }

    fn error_count(&self) -> u16 {
        self.iso_packet_giveback()
            .iter()
            .filter(|pkt| pkt.status != 0)
            .count() as u16
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
    fn urb_reply(
        &mut self,
        seqnum: u32,
        data: Data<UrbFrame>,
    ) -> impl Future<Output = io::Result<()>> + 'static {
        let mut vhci = self.vhci.clone();
        let id = self.id;
        let handle = {
            let mut guard = self.handle_seqnum_map.lock().unwrap();
            let (seqnums, handles) = guard.deref_mut();
            let handle = handles.remove(&seqnum).unwrap();
            _ = seqnums.remove(&handle).unwrap();
            handle
        };

        let (urb, data) = data.split::<[u8]>();
        let mut data = data.unwrap().into_bytes_mut();

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
        };

        if vhci::Status::Success != urb.status {
            let _enter = warn_span!("urb_reply").entered();
            warn!(
                "({id}) {:?} {:?} transfer {seqnum} failed: {:?}",
                urb.kind,
                urb.endpoint.direction(),
                urb.status
            );
        }

        vhci.giveback_urb(giveback)
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
    W: AsyncWrite + CloseStream + Unpin + Send + 'static,
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
            IntMap::with_capacity_and_hasher(512, Default::default()),
        )));
        let cloned_map = Arc::clone(&map);
        let send_loop = borrow::SendLoop::new(tx, work_rx);
        let send_handler = BorrowSendHandler::new(vhci.remote(), id, map);
        let send = compio_runtime::spawn(
            send_loop
                .run(Box::new(send_handler), cancel_token.clone())
                .in_current_span(),
        );
        let recv_loop = borrow::RecvLoop::new(rx, buf_rx);
        let recv_handler = BorrowRecvHandler::new(vhci.remote(), id, cloned_map);
        let recv = compio_runtime::spawn(
            recv_loop
                .run(Box::new(recv_handler), cancel_token.clone())
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

pub(crate) fn open_device(dev_id: msg::UsbDeviceId) -> rusb::Result<lend::device::Handle> {
    lend::device::open(dev_id)
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
            let _guard = tracing::Span::current().entered();
            // TODO: This doesn't really return the actual errno, as:
            // 1. The processing usually happens on another thread, and
            // 2. Even if the processing happened on the same thread, this
            // function is not the next logical thing to run after.
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap();
            warn! { %err, %errno, "({seqnum}) {kind:?} transfer failed on {dev_id:?}" };
            vhci::Status::from_errno_raw(-errno, UrbType::Iso == kind)
        }
    }
}

#[derive(Debug, Clone, Copy)]
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

#[derive(Default, Clone)]
struct BigTransfers {
    inner: Rc<RefCell<BTreeMap<u16, OneOrMany<LibusbTransfer2>>>>,
}

impl BigTransfers {
    fn insert(&mut self, transfer: LibusbTransfer2) {
        const EMPTY: OneOrMany<LibusbTransfer2> = OneOrMany::Many(Vec::new());
        let mut cache = self.inner.borrow_mut();
        let key = transfer.max_iso_packets() as u16;
        let entry = cache.get_mut(&key);
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
                let value = OneOrMany::One(transfer);
                cache.insert(key, value);
            }
        }
    }

    fn remove(&mut self, num_iso_packets: u16) -> Option<LibusbTransfer2> {
        let mut cache = self.inner.borrow_mut();
        let maybe_entry = cache.range_mut(num_iso_packets..).next();
        if let Some((&num_pkts, entry)) = maybe_entry {
            const EMPTY: OneOrMany<LibusbTransfer2> = OneOrMany::Many(Vec::new());
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
                cache.remove(&num_pkts);
            }
            Some(transfer)
        } else {
            None
        }
    }
}

#[derive(Default, Clone)]
struct SmallTransfers {
    inner: Rc<RefCell<Vec<LibusbTransfer2>>>,
}

impl SmallTransfers {
    fn remove(&mut self) -> Option<LibusbTransfer2> {
        self.inner.borrow_mut().pop()
    }

    fn insert(&mut self, transfer: LibusbTransfer2) {
        self.inner.borrow_mut().push(transfer);
    }
}

impl From<Vec<LibusbTransfer2>> for SmallTransfers {
    fn from(value: Vec<LibusbTransfer2>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(value)),
        }
    }
}

struct LendRecvHandler {
    dev_id: msg::UsbDeviceId,
    active_transfers: Rc<RefCell<IntMap<u32, CancelTransferToken>>>,
    cached_cancel_tokens: Rc<RefCell<Vec<CancelTransferToken>>>,
    device: Arc<lend::device::Handle>,
    scratch: BytesAllocator,
    isos: BigTransfers,
    smalls: SmallTransfers,
}

impl LendRecvHandler {
    #[inline]
    fn register_cancel_token(&mut self, seqnum: u32) -> CancelTransferToken {
        let cancel = self
            .cached_cancel_tokens
            .borrow_mut()
            .pop()
            .unwrap_or_else(CancelTransferToken::new);
        self.active_transfers
            .borrow_mut()
            .insert(seqnum, cancel.clone());
        cancel
    }
}

impl lend::RecvHandler for LendRecvHandler {
    fn cancel_urb(&mut self, seqnum: u32) {
        if let Some(transfer) = self.active_transfers.borrow_mut().remove(&seqnum) {
            transfer.cancel();
        }
    }

    fn device_reset(
        &mut self,
        seqnum: u32,
    ) -> impl Future<Output = Seq<proto::msg::Status>> + 'static {
        async move {
            Seq {
                seqnum,
                data: msg::Status::Success,
            }
        }
    }

    fn set_config(
        &mut self,
        Seq {
            seqnum,
            data: SetConfig { desired },
        }: Seq<SetConfig>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let device = Arc::clone(&self.device);
        async move {
            let Seq {
                seqnum,
                data: status,
            } = device.set_config_async(seqnum, desired).await;
            Seq {
                seqnum,
                data: Ctrl {
                    res: ResultData::Out {
                        bytes_transferred: 0,
                    },
                    endpoint: ioctl::Endpoint(vhci::usbfs::Dir::Out as u8),
                    status,
                },
            }
        }
    }

    fn set_interface(
        &mut self,
        Seq {
            seqnum,
            data: SetInterface { setting, interface },
        }: Seq<SetInterface>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let device = Arc::clone(&self.device);
        async move {
            let Seq {
                seqnum,
                data: status,
            } = device
                .set_alt_setting_async(seqnum, interface, setting)
                .await;
            Seq {
                seqnum,
                data: Ctrl {
                    res: ResultData::Out {
                        bytes_transferred: 0,
                    },
                    endpoint: ioctl::Endpoint(vhci::usbfs::Dir::Out as u8),
                    status,
                },
            }
        }
    }

    fn clear_stall(
        &mut self,
        Seq {
            seqnum,
            data: ClearStall { endpoint },
        }: Seq<ClearStall>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let device = Arc::clone(&self.device);
        async move {
            let Seq {
                seqnum,
                data: status,
            } = device.clear_stall_async(seqnum, endpoint).await;
            Seq {
                seqnum,
                data: Ctrl {
                    res: ResultData::Out {
                        bytes_transferred: 0,
                    },
                    endpoint: ioctl::Endpoint(vhci::usbfs::Dir::Out as u8),
                    status,
                },
            }
        }
    }

    fn new_ctrl(
        &mut self,
        Seq {
            seqnum,
            data: urb_frame,
        }: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let mut cancel = self.register_cancel_token(seqnum);
        let header = urb_frame.get().header();
        let actual_transfer_len = header.actual_transfer_len as usize;
        let ctrl_pkt = header.ctrl_packet;
        let mut buf = if header.is_out() {
            let w_length = ctrl_pkt.length() as usize;
            let mut buf = urb_frame.into_bytes_mut();
            const SKIP_HEADER: usize = size_of::<UrbHeader>() - size_of::<ioctl::IocSetupPacket>();
            buf.advance(SKIP_HEADER);
            assert_eq!(
                align_to_usize(w_length + size_of::<ioctl::IocSetupPacket>()),
                buf.len()
            );
            buf
        } else {
            let mut buf = self
                .scratch
                .reserve(size_of::<ioctl::IocSetupPacket>() + header.padded_transfer_len());
            buf[..size_of::<ioctl::IocSetupPacket>()].copy_from_slice(ctrl_pkt.as_bytes());
            buf
        };
        let is_get_status = Request::STANDARD_DEVICE_GET_STATUS == ctrl_pkt.req();

        buf.truncate(size_of::<ioctl::IocSetupPacket>() + actual_transfer_len);
        let transfer = self
            .smalls
            .remove()
            .unwrap_or_else(|| LibusbTransfer2::new_with_zero_packets());

        // SAFETY: Transfer buf is exactly the length needed for a control
        // transfer, and its capacity matches w_length + 8.
        let transfer =
            unsafe { transfer.into_ctrl(self.device.as_device(), buf, Duration::from_millis(900)) };

        let mut cache = self.smalls.clone();
        let dev_id = self.dev_id;
        let dir = ctrl_pkt.req().dir();
        let endpoint = match dir {
            Dir::Out => ioctl::Endpoint(0),
            Dir::In => ioctl::Endpoint(128),
        };

        let transfer = Arc::new(transfer);
        let ours = Arc::clone(&transfer);
        task::spawn_blocking(move || {
            _ = transfer.try_submit();
        });
        async move {
            let result = ours.wait(&mut cancel).await;
            let status = convert_libusb_to_vhci(Ok(result), UrbType::Ctrl, seqnum, dev_id);

            let mut buf = {
                let (transfer, mut buf) =
                    Arc::into_inner(ours).and_then(Transfer2::complete).unwrap();
                cache.insert(transfer);
                buf.advance(size_of::<ioctl::IocSetupPacket>());
                buf
            };

            if is_get_status {
                // Indicate that our fake USB device is self powered.
                buf[0] = 0x01;
            }

            let res = ResultData::new(buf, dir);
            Seq {
                seqnum,
                data: Ctrl {
                    res,
                    status,
                    endpoint,
                },
            }
        }
    }

    fn new_int(
        &mut self,
        Seq {
            seqnum,
            data: urb_frame,
        }: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Int>> + 'static {
        let mut cancel = self.register_cancel_token(seqnum);
        let (header, data) = urb_frame.split::<[u8]>();
        let mut buf = if header.is_out() {
            data.unwrap().into_bytes_mut()
        } else {
            self.scratch.reserve(header.padded_transfer_len())
        };

        buf.truncate(header.actual_transfer_len as usize);
        let transfer = self
            .smalls
            .remove()
            .unwrap_or_else(|| LibusbTransfer2::new_with_zero_packets());
        let endpoint = header.endpoint;
        let interval = header.interval;

        // SAFETY: Transfer buffer has a capacity of actual_transfer_len.
        let transfer = unsafe { transfer.into_int(self.device.as_device(), endpoint.0, buf) };

        let mut cache = self.smalls.clone();
        let dev_id = self.dev_id;

        let transfer = Arc::new(transfer);
        let ours = Arc::clone(&transfer);
        task::spawn_blocking(move || {
            _ = transfer.try_submit();
        });
        async move {
            let result = ours.wait(&mut cancel).await;
            let status = convert_libusb_to_vhci(Ok(result), UrbType::Int, seqnum, dev_id);

            let (transfer, buf) = Arc::into_inner(ours).and_then(Transfer2::complete).unwrap();
            cache.insert(transfer);

            let res = ResultData::new(buf, endpoint.direction());
            Seq {
                seqnum,
                data: Int {
                    res,
                    endpoint,
                    interval,
                    status,
                },
            }
        }
    }

    fn new_iso(
        &mut self,
        Seq {
            seqnum,
            data: urb_frame,
        }: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Iso>> + 'static {
        #[repr(transparent)]
        struct Pkt(ioctl::IocIsoPacketData);
        impl IsoPacket for Pkt {
            fn len(&self) -> u32 {
                self.0.packet_length
            }
        }

        #[repr(transparent)]
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

        let mut cancel = self.register_cancel_token(seqnum);

        // BUG: By specifying the type as a `[u8]`,
        // the inner NonNull pointer inside becomes a fat
        // pointer, and I just wonder if that's what I'm
        // looking for? Because most pointers to a slice
        // have the type `NonNull<u8>`, not `NonNull<[u8]>`.
        // Hmmmmmmmmmm...
        let (header, data) = urb_frame.split::<[u8]>();
        let mut data = data.unwrap().into_bytes_mut();
        let padded_transfer_len = header.padded_transfer_len();

        let (mut transfer_buf, mut raw_iso_buf) = if header.is_out() {
            let transfer_buf = data.split_to(padded_transfer_len);
            (transfer_buf, data)
        } else {
            let transfer_buf = self.scratch.reserve(padded_transfer_len);
            (transfer_buf, data)
        };

        transfer_buf.truncate(header.actual_transfer_len as usize);
        let num_iso_pkts = header.iso_packet_count as usize;
        let iso_pkts =
            <[ioctl::IocIsoPacketData]>::ref_from_bytes_with_elems(&raw_iso_buf[..], num_iso_pkts)
                .unwrap();
        let transfer = self
            .isos
            .remove(header.iso_packet_count)
            .unwrap_or_else(|| LibusbTransfer2::new(num_iso_pkts));
        let endpoint = header.endpoint;
        let interval = header.interval;
        let transfer = unsafe {
            transfer.into_iso(
                self.device.as_device(),
                endpoint.0,
                transfer_buf,
                Iter {
                    pkts: iso_pkts.iter(),
                },
            )
        };

        let mut cache = self.isos.clone();
        let dev_id = self.dev_id;

        let transfer = Arc::new(transfer);
        let ours = Arc::clone(&transfer);
        task::spawn_blocking(move || {
            _ = transfer.try_submit();
        });
        async move {
            let result = ours.wait(&mut cancel).await;
            let status = convert_libusb_to_vhci(Ok(result), UrbType::Iso, seqnum, dev_id);

            let our_pkts = <[ioctl::IocIsoPacketGiveback]>::mut_from_bytes_with_elems(
                &mut raw_iso_buf[..],
                num_iso_pkts,
            )
            .unwrap();
            let their_pkts = ours
                .iso_packets()
                .expect("why wouldn't a transfer be done by this point?");

            let num_errors =
                our_pkts
                    .iter_mut()
                    .zip(their_pkts)
                    .fold(0, |num_errors, (our_pkt, their_pkt)| {
                        our_pkt.packet_actual = their_pkt.actual_len();
                        our_pkt.status =
                            vhci_from_transfer_status(their_pkt.status()).to_errno_raw(true);
                        num_errors + (our_pkt.status != 0) as u16
                    });

            let (transfer, mut buf) = Arc::into_inner(ours).and_then(Transfer2::complete).unwrap();
            cache.insert(transfer);

            // SAFETY: Isochronous transfer requires that the full
            // buffer be sent back to the caller.
            unsafe { buf.set_len(buf.capacity()) };

            let res = ResultData::new(buf, endpoint.direction());
            Seq {
                seqnum,
                data: Iso {
                    res,
                    endpoint,
                    interval,
                    raw_iso_buf,
                    num_errors,
                    num_iso_packets: num_iso_pkts as u16,
                    status,
                },
            }
        }
    }

    fn new_bulk(
        &mut self,
        Seq {
            seqnum,
            data: urb_frame,
        }: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Bulk>> + 'static {
        let mut cancel = self.register_cancel_token(seqnum);
        let (header, data) = urb_frame.split::<[u8]>();
        let mut buf = if header.is_out() {
            data.unwrap().into_bytes_mut()
        } else {
            self.scratch.reserve(header.padded_transfer_len())
        };

        buf.truncate(header.actual_transfer_len as usize);
        let transfer = self
            .smalls
            .remove()
            .unwrap_or_else(|| LibusbTransfer2::new_with_zero_packets());
        let endpoint = header.endpoint;

        // SAFETY: Transfer buffer has a capacity of actual_transfer_len.
        let transfer = unsafe {
            transfer.into_bulk(
                self.device.as_device(),
                endpoint.0,
                TransferFlags::NONE,
                buf,
            )
        };

        let mut cache = self.smalls.clone();
        let dev_id = self.dev_id;
        let transfer = Arc::new(transfer);
        let ours = Arc::clone(&transfer);
        task::spawn_blocking(move || {
            _ = transfer.try_submit();
        });
        async move {
            let result = ours.wait(&mut cancel).await;
            let status = convert_libusb_to_vhci(Ok(result), UrbType::Bulk, seqnum, dev_id);

            let (transfer, buf) = Arc::into_inner(ours).and_then(Transfer2::complete).unwrap();
            cache.insert(transfer);

            let res = ResultData::new(buf, endpoint.direction());
            Seq {
                seqnum,
                data: Bulk {
                    res,
                    endpoint,
                    status,
                },
            }
        }
    }
}

impl Drop for LendRecvHandler {
    fn drop(&mut self) {
        self.active_transfers
            .take()
            .into_values()
            .for_each(|transfer| transfer.cancel());
    }
}

const THRESHOLD: usize = 64;

struct LendRecvStats {
    last_few_setup_times: RollingAvg<THRESHOLD, metrics::Duration>,
    max_setup_times: Max<THRESHOLD, Duration, UrbType>,
    min_setup_times: Min<THRESHOLD, Duration>,
    num_active_transfers: Arc<AtomicU32>,
    max_active_transfers: u32,
}

impl LendRecvStats {
    fn new() -> (Self, Arc<AtomicU32>) {
        let num_active_transfers = Arc::new(AtomicU32::new(0));
        let this = Self {
            last_few_setup_times: RollingAvg::preallocated(),
            max_setup_times: Max::new(),
            min_setup_times: Min::new(),
            num_active_transfers: Arc::clone(&num_active_transfers),
            max_active_transfers: 0,
        };
        (this, num_active_transfers)
    }

    fn mark_start(&mut self) -> Instant {
        let num_active = self.num_active_transfers.fetch_add(1, Ordering::AcqRel);
        self.max_active_transfers = std::cmp::max(self.max_active_transfers, num_active + 1);
        Instant::now()
    }

    fn mark_end(&mut self, start: Instant, header: UrbType) {
        let elapsed = start.elapsed();
        self.last_few_setup_times.push(elapsed);
        self.max_setup_times.push(elapsed, header);
        self.min_setup_times.push(elapsed);
    }
}

impl Drop for LendRecvStats {
    fn drop(&mut self) {
        debug!("==== RECV STATS ====");
        debug!("Max active transfers: {}", self.max_active_transfers);
        debug!("Min setup times: {:?}", self.min_setup_times);
        debug!("Max setup times: {:?}", self.max_setup_times);
        debug!(
            "Avg setup time for last {} transfers: {:?}",
            self.last_few_setup_times.len(),
            Duration::from(self.last_few_setup_times.mean().unwrap_or_default())
        );
    }
}

struct StatLendRecvHandler<H> {
    inner: H,
    stats: LendRecvStats,
}

impl<H: lend::RecvHandler> lend::RecvHandler for StatLendRecvHandler<H> {
    fn cancel_urb(&mut self, seqnum: u32) {
        self.inner.cancel_urb(seqnum);
    }

    fn device_reset(
        &mut self,
        seqnum: u32,
    ) -> impl Future<Output = Seq<proto::msg::Status>> + 'static {
        self.inner.device_reset(seqnum)
    }

    fn set_config(&mut self, data: Seq<SetConfig>) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.set_config(data);
        self.stats.mark_end(start, UrbType::Ctrl);
        fut
    }

    fn set_interface(
        &mut self,
        data: Seq<SetInterface>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.set_interface(data);
        self.stats.mark_end(start, UrbType::Ctrl);
        fut
    }

    fn clear_stall(&mut self, data: Seq<ClearStall>) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.clear_stall(data);
        self.stats.mark_end(start, UrbType::Ctrl);
        fut
    }

    fn new_ctrl(
        &mut self,
        frame: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.new_ctrl(frame);
        self.stats.mark_end(start, UrbType::Ctrl);
        fut
    }

    fn new_int(&mut self, frame: Seq<Data<UrbFrame>>) -> impl Future<Output = Seq<Int>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.new_int(frame);
        self.stats.mark_end(start, UrbType::Int);
        fut
    }

    fn new_iso(&mut self, frame: Seq<Data<UrbFrame>>) -> impl Future<Output = Seq<Iso>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.new_iso(frame);
        self.stats.mark_end(start, UrbType::Iso);
        fut
    }

    fn new_bulk(
        &mut self,
        frame: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Bulk>> + 'static {
        let start = self.stats.mark_start();
        let fut = self.inner.new_bulk(frame);
        self.stats.mark_end(start, UrbType::Bulk);
        fut
    }
}

struct LendSendHandler {
    dev_id: msg::UsbDeviceId,
    buf: BytesMut,
    active_transfers: Rc<RefCell<IntMap<u32, CancelTransferToken>>>,
    cached_cancel_tokens: Rc<RefCell<Vec<CancelTransferToken>>>,
}

impl LendSendHandler {
    fn deregister_token(&mut self, seqnum: u32) {
        if let Some(token) = self.active_transfers.borrow_mut().remove(&seqnum) {
            debug_assert!(!token.is_cancelled());
            self.cached_cancel_tokens.borrow_mut().push(token);
        }
    }
}

impl lend::SendHandler for LendSendHandler {
    fn is_buf_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn flush_buf(&mut self) -> Bytes {
        self.buf.split().freeze()
    }

    fn iso_completed(
        &mut self,
        Seq {
            seqnum,
            data:
                Iso {
                    res,
                    endpoint,
                    interval,
                    raw_iso_buf,
                    num_errors,
                    num_iso_packets,
                    status: vhci_status,
                },
        }: Seq<Iso>,
    ) -> std::result::Result<(), lend::Error> {
        self.deregister_token(seqnum);
        let status = msg::Status::from(vhci_status);
        if msg::Status::Success != status {
            let err = lend::Error::Usb(lend::UsbError {
                id: self.dev_id,
                seqnum,
                status,
                kind: lend::FrameKind::Transfer {
                    kind: UrbType::Iso,
                    endpoint,
                    status: vhci_status,
                },
            });
            return Err(err);
        }

        let lend::HeaderData {
            actual_transfer_len,
            transfer,
            padding,
        } = res.as_header_data();

        // If we're not sending any transfer data then this will
        // just be the guaranteed length.
        let total_frame_len = {
            let guaranteed = size_of::<Header>() + size_of::<UrbHeader>() + raw_iso_buf.len();
            let len = guaranteed + transfer.len() + padding.len();
            compress_frame_len(len)
        };

        let header = Header {
            total_frame_len,
            command: msg::Command::RetSubmit,
            status,
            seqnum,
        };
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

        self.buf.put_slice(header.as_bytes());
        self.buf.put_slice(urb_header.as_bytes());
        self.buf.put_slice(transfer);
        self.buf.put_slice(padding);
        self.buf.put_slice(&raw_iso_buf);
        Ok(())
    }

    fn int_completed(
        &mut self,
        Seq {
            seqnum,
            data:
                Int {
                    res,
                    endpoint,
                    interval,
                    status: vhci_status,
                },
        }: Seq<Int>,
    ) -> std::result::Result<(), lend::Error> {
        self.deregister_token(seqnum);
        let status = msg::Status::from(vhci_status);
        if msg::Status::Success != status {
            let err = lend::Error::Usb(lend::UsbError {
                id: self.dev_id,
                seqnum,
                status,
                kind: lend::FrameKind::Transfer {
                    kind: UrbType::Int,
                    endpoint,
                    status: vhci_status,
                },
            });
            return Err(err);
        }

        let lend::HeaderData {
            actual_transfer_len,
            transfer,
            padding,
        } = res.as_header_data();

        // If we're not sending any transfer data then this will
        // just be the guaranteed length.
        let total_frame_len = {
            let guaranteed = size_of::<Header>() + size_of::<UrbHeader>();
            let len = guaranteed + transfer.len() + padding.len();
            compress_frame_len(len)
        };

        let header = Header {
            total_frame_len,
            command: msg::Command::RetSubmit,
            status,
            seqnum,
        };
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

        self.buf.put_slice(header.as_bytes());
        self.buf.put_slice(urb_header.as_bytes());
        self.buf.put_slice(transfer);
        self.buf.put_slice(padding);
        Ok(())
    }

    fn ctrl_completed(
        &mut self,
        Seq {
            seqnum,
            data:
                Ctrl {
                    res,
                    endpoint,
                    status: vhci_status,
                },
        }: Seq<Ctrl>,
    ) -> std::result::Result<(), lend::Error> {
        self.deregister_token(seqnum);
        let status = msg::Status::from(vhci_status);
        if msg::Status::Success != status {
            let err = lend::Error::Usb(lend::UsbError {
                id: self.dev_id,
                seqnum,
                status,
                kind: lend::FrameKind::Transfer {
                    kind: UrbType::Ctrl,
                    endpoint,
                    status: vhci_status,
                },
            });
            return Err(err);
        }

        let lend::HeaderData {
            actual_transfer_len,
            transfer,
            padding,
        } = res.as_header_data();

        // If we're not sending any transfer data then this will
        // just be the guaranteed length.
        let total_frame_len = {
            let guaranteed = size_of::<Header>() + size_of::<UrbHeader>();
            let len = guaranteed + transfer.len() + padding.len();
            compress_frame_len(len)
        };

        let header = Header {
            total_frame_len,
            command: msg::Command::RetSubmit,
            status,
            seqnum,
        };
        let urb_header = UrbHeader {
            actual_transfer_len,
            iso_packet_count: 0,
            endpoint,
            kind: UrbType::Ctrl,
            interval: 0,
            status: vhci_status,
            flags: 0,
            num_errors: 0,
            ctrl_packet: Default::default(),
        };

        self.buf.put_slice(header.as_bytes());
        self.buf.put_slice(urb_header.as_bytes());
        self.buf.put_slice(transfer);
        self.buf.put_slice(padding);
        Ok(())
    }

    fn bulk_completed(
        &mut self,
        Seq {
            seqnum,
            data:
                Bulk {
                    res,
                    endpoint,
                    status: vhci_status,
                },
        }: Seq<Bulk>,
    ) -> std::result::Result<(), lend::Error> {
        self.deregister_token(seqnum);
        let status = msg::Status::from(vhci_status);
        if msg::Status::Success != status {
            let err = lend::Error::Usb(lend::UsbError {
                id: self.dev_id,
                seqnum,
                status,
                kind: lend::FrameKind::Transfer {
                    kind: UrbType::Bulk,
                    endpoint,
                    status: vhci_status,
                },
            });
            return Err(err);
        }

        let lend::HeaderData {
            actual_transfer_len,
            transfer,
            padding,
        } = res.as_header_data();

        // If we're not sending any transfer data then this will
        // just be the guaranteed length.
        let total_frame_len = {
            let guaranteed = size_of::<Header>() + size_of::<UrbHeader>();
            let len = guaranteed + transfer.len() + padding.len();
            compress_frame_len(len)
        };

        let header = Header {
            total_frame_len,
            command: msg::Command::RetSubmit,
            status,
            seqnum,
        };
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

        self.buf.put_slice(header.as_bytes());
        self.buf.put_slice(urb_header.as_bytes());
        self.buf.put_slice(transfer);
        self.buf.put_slice(padding);
        Ok(())
    }

    fn device_resetted(
        &mut self,
        Seq {
            seqnum,
            data: status,
        }: Seq<proto::msg::Status>,
    ) -> std::result::Result<(), lend::Error> {
        if msg::Status::Success == status {
            self.buf.put_u64_le(transmute!(Header {
                command: msg::Command::RetPort,
                status,
                total_frame_len: compress_frame_len(size_of::<Header>()),
                seqnum
            }));
            Ok(())
        } else {
            Err(lend::Error::Usb(lend::UsbError {
                id: self.dev_id,
                seqnum,
                status,
                kind: lend::FrameKind::Reset,
            }))
        }
    }
}

struct LendSendStats {
    num_transfers_completed: u32,
    num_active_transfers: Arc<AtomicU32>,
    max_completed_transfers_before_flush: u32,
    num_iso_transfers: u64,
}

impl LendSendStats {
    fn new(num_active_transfers: Arc<AtomicU32>) -> Self {
        Self {
            num_transfers_completed: 0,
            num_active_transfers,
            max_completed_transfers_before_flush: 0,
            num_iso_transfers: 0,
        }
    }

    fn mark_flush(&mut self) {
        self.max_completed_transfers_before_flush = std::cmp::max(
            self.max_completed_transfers_before_flush,
            self.num_transfers_completed,
        );
        self.num_transfers_completed = 0;
    }

    fn mark_complete(&mut self) {
        self.num_active_transfers.fetch_sub(1, Ordering::AcqRel);
        self.num_transfers_completed += 1;
    }

    fn mark_iso(&mut self) {
        self.num_iso_transfers += 1;
    }
}

impl Drop for LendSendStats {
    fn drop(&mut self) {
        debug!("==== SEND STATS ====");
        debug!(
            "Max completed transfers before flush: {}",
            self.max_completed_transfers_before_flush
        );
        debug!(
            "Total number of isochronous transfers: {}",
            self.num_iso_transfers
        );
    }
}

struct StatLendSendHandler<H> {
    inner: H,
    stats: LendSendStats,
}

impl<H: lend::SendHandler> lend::SendHandler for StatLendSendHandler<H> {
    fn is_buf_empty(&self) -> bool {
        self.inner.is_buf_empty()
    }

    fn flush_buf(&mut self) -> Bytes {
        self.stats.mark_flush();
        self.inner.flush_buf()
    }

    fn iso_completed(&mut self, iso: Seq<Iso>) -> std::result::Result<(), lend::Error> {
        self.stats.mark_complete();
        self.stats.mark_iso();
        self.inner.iso_completed(iso)
    }

    fn int_completed(&mut self, int: Seq<Int>) -> std::result::Result<(), lend::Error> {
        self.stats.mark_complete();
        self.inner.int_completed(int)
    }

    fn ctrl_completed(&mut self, ctrl: Seq<Ctrl>) -> std::result::Result<(), lend::Error> {
        self.stats.mark_complete();
        self.inner.ctrl_completed(ctrl)
    }

    fn bulk_completed(&mut self, bulk: Seq<Bulk>) -> std::result::Result<(), lend::Error> {
        self.stats.mark_complete();
        self.inner.bulk_completed(bulk)
    }

    fn device_resetted(
        &mut self,
        reset: Seq<proto::msg::Status>,
    ) -> std::result::Result<(), lend::Error> {
        self.inner.device_resetted(reset)
    }
}

pub struct LendDevice<W, R> {
    tx: W,
    rx: R,
    buf_rx: Ring,
    device: lend::device::Handle,
    id: msg::UsbDeviceId,
}

impl<W, R> LendDevice<W, R> {
    pub fn new(
        tx: W,
        rx: R,
        buf_rx: Ring,
        device: lend::device::Handle,
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
    W: AsyncWrite + CloseStream + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn lend(self, cancel_token: CancellationToken) -> Result<()> {
        let Self {
            tx,
            rx,
            buf_rx,
            device,
            id: dev_id,
        } = self;

        let device = Arc::new(device);
        let our_device = Arc::clone(&device);
        let ctx = device.as_device().context().clone();
        let span = warn_span!("libusb_event_handler");
        let runtime = cancel_token.clone();
        let libusb = compio_runtime::spawn_blocking(move || {
            let _guard = span.entered();
            trace!("({dev_id} started)");
            while !runtime.is_cancelled() {
                if let Err(err) = ctx.handle_events(Some(Duration::from_secs(5))) {
                    warn! { %err };
                }
            }
            trace!("({dev_id} complete)");
        });

        let active_transfers = Rc::new(RefCell::new(IntMap::with_capacity_and_hasher(
            32,
            Default::default(),
        )));
        let cached_cancel_tokens = Rc::new(RefCell::new(Vec::with_capacity(8)));

        const BUF_LEN: usize = u16::MAX as usize;
        let (recv_stats, num_active_transfers) = LendRecvStats::new();
        let recv_handler = LendRecvHandler {
            dev_id,
            device,
            active_transfers: active_transfers.clone(),
            cached_cancel_tokens: cached_cancel_tokens.clone(),
            scratch: BytesAllocator::with_capacity(2),
            isos: BigTransfers::default(),
            smalls: vec![
                LibusbTransfer2::new_with_zero_packets(),
                LibusbTransfer2::new_with_zero_packets(),
            ]
            .into(),
        };
        let recv_handler = StatLendRecvHandler {
            inner: recv_handler,
            stats: recv_stats,
        };

        let send_stats = LendSendStats::new(num_active_transfers);
        let send_handler = LendSendHandler {
            dev_id,
            buf: BytesMut::with_capacity(BUF_LEN),
            active_transfers,
            cached_cancel_tokens,
        };
        let send_handler = StatLendSendHandler {
            inner: send_handler,
            stats: send_stats,
        };

        let (send, recv) = lend::loops(tx, rx, buf_rx);

        let recv = compio_runtime::spawn(
            recv.run(Box::new(recv_handler), cancel_token.clone())
                .in_current_span(),
        );
        let send = compio_runtime::spawn(
            send.run(Box::new(send_handler), cancel_token.clone())
                .in_current_span(),
        );
        let (recv_result, send_result) = (recv, send).join().await;

        info!("({dev_id}) shutting down");

        recv_result.unwrap()?;
        send_result.unwrap()?;

        cancel_token.cancel();
        drop(our_device);
        libusb.await.unwrap();

        Ok(())
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

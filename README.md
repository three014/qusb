# Qusb: A USB/QUIC Peer-to-Peer Runtime

> [!WARNING]  
> **This crate is in active development. Do not expect anything in this crate to stay the same for long.**

Allows peers to share USB devices over the internet. Uses QUIC's encrypted and ordered communication for
control, interrupt, bulk, and isochronous transfers.

# Main goal: Get this thing to work

Right now the crate is incomplete. I am still designing the project and am hoping to have a working
version of this crate by the end of January 2025.

**Update 01/15/2025**: Control and Interrupt transfers work! With some modifications to the
`rusb` crate and the addition of my own `rusb-async` crate, I took advantage of libusb's
asynchronous transfer api. 

**Update 01/27/2025**: Mass storage devices work! However I haven't figured out how to smoothly
allow the OS to unmount the drives, so for now I believe unmounting a drive may slightly corrupt
the data on the drive. :D

# Dev Setup Guide

Unfortunately this is a Linux project only, and it might stay this way for a while.

First, you'll need `cargo` and Rust installed on your machine. I've been using version 1.83.0,
but I saw that 1.84.0 [was just released](https://doc.rust-lang.org/stable/releases.html#version-1840-2025-01-09).
While I haven't tested this project with that version, I'm certain that this project should still work.

You're also going to need your system's kernel headers and the development libraries to build kernel modules.
This is for the `vhci-hcd` and `vhci-iocifc` modules. In the future, the modules will only be needed if your machine is
the one borrowing a USB device, but for now `qusb` assumes you can borrow and lend USB devices.

Finally, you'll need the `libusb` library installed for lending USB devices. This requirement is for the same reason
as with the vhci modules, but I feel like developers are more likely to have `libusb` already installed.

Next, clone the `vhci-hcd` repo and make the kernel modules.

```
git clone https://github.com/three014/vhci-hcd.git
cd ./vhci-hcd
make
```

If the build succeeded, you can insert the modules with `modprobe`.

```
sudo modprobe ./usb-vhci-hcd.ko
sudo modprobe ./usb-vhci-iocifc.ko
```

Now back to this repo. The `crates/qusb/` folder is where the main project code is, and
`cargo build` should be all you need to get it working. 

```
git clone https://github.com/three014/qusb.git
cd qusb/crates/qusb
cargo build
```

Once you have the project built, then 
take a look at the integration tests in the `tests/` folder.

To run a test, you need root privleges for now, unless you change the permissions
on `/dev/vhci` (I believe) and change the udev rules for USB device access.

Here's an example test run for sharing a USB device on the same machine. 

Before running this test, use `lsusb` to choose a device to test, then edit the test
`borrow_self_dev()` in `tests/integration_test.rs` to use the correct `bus_number` 
and `device_addr`.

```
cd crates/qusb
./cargo_su.sh test borrow_self_dev &
tail -f log.txt
```

The `log.txt` in your current directory will show the output of the USB transfers
between the borrow/lend processes, and your USB device should "reconnect" to your
computer under a new id. You can also use `sudo dmesg -Hw` to see the new USB
device recognized by your OS.

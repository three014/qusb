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

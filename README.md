# Qusb: A USB/QUIC Peer-to-Peer Runtime

> [!WARNING]  
> **This crate is in active development. Do not expect anything in this crate to stay the same for long.**

Allows peers to share USB devices over the internet. Uses QUIC's encrypted and ordered communication for
interrupt, bulk, and control transfers. Uses QUIC's unreliable datagrams 
([RFC 9221](https://datatracker.ietf.org/doc/html/rfc9221)) for isochronous transfer.

# Main goal: Get this thing to work

Right now the crate is incomplete. I am still designing the project and am hoping to have a working
version of this crate by the end of January 2025.

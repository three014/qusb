// OKAY what do we want to do again?
//
// ACTIVE: Send an offer to a borrower
// PASSIVE: Listen for requests from borrowers
//          Optionally send a list of available
//            devices if asked by anyone
//
// We want to let the user of this library
// create the QUIC config themselves,
// then pass control to us to either make
// the offer or listen.

use quinn;

/// Uses the provided endpoint to connect to the remote peer and offer to lend
/// the specified USB device.
///
/// The interface is reference counted, but this function will assume for
/// now that nowhere else in the program is going to send/recv URB data
/// to this interface. So please don't do that. The reason why this
/// function asks for the already-opened interface is to give the user
/// the maximum flexibility in what device they want to offer to the
/// borrower.
///
/// A few things can go wrong in this function. For one, the QUIC
/// endpoint may fail to connect for a million different reasons.
/// Or, maybe the borrower declined the offer.
///
/// Many things may also go wrong on the USB side as well. Maybe someone
/// unplugged the device, and now we can't offer it anymore. Or maybe
/// another program claimed the interface *right* before we did.
///
/// The function returns immediately because its async, but on 
/// completion of the async, the user gets a handle to the qusb connection.
/// The user can close the connection whenever they want, it's okay.
pub async fn send_offer(endpoint: quinn::Endpoint, usb_device: nusb::Interface) {
    
}

#[cfg(test)]
mod tests {}

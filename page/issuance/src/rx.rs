use crate::error::IssuedRxError;
use crate::pool::IssuancePool;
use crate::wire::IssuedOwnedFrame;

/// Receiver wrapper that keeps issuance permits attached to accepted pages.
#[derive(Clone)]
pub struct IssuedRx {
    rx: transfer::PageRx,
    permits: IssuancePool,
}

/// Result of accepting one issued transport frame.
pub enum IssueEvent {
    Page(IssuedReceivedPage),
    Closed,
}

/// Received page paired with its outstanding issuance permit.
pub struct IssuedReceivedPage {
    page: Option<transfer::ReceivedPage>,
    permits: IssuancePool,
    permit_id: u32,
    released: bool,
}

impl IssuedRx {
    /// Create a permit-aware receiver over an attached [`transfer::PageRx`].
    pub fn new(rx: transfer::PageRx, permits: IssuancePool) -> Self {
        Self { rx, permits }
    }

    /// Accept one decoded issued frame.
    ///
    /// The frame is borrowed so callers may retry the same page frame after a
    /// retryable receiver error such as [`transfer::RxError::Busy`] or
    /// [`transfer::RxError::UnexpectedTransferId`]. Permits are released only
    /// on successful page ownership transfer or on fatal accept errors that
    /// definitively reclaim the detached page.
    pub fn accept(&self, frame: &IssuedOwnedFrame) -> Result<IssueEvent, IssuedRxError> {
        match *frame {
            IssuedOwnedFrame::Close(frame) => {
                let event = self.rx.accept(transfer::OwnedFrame::Close(frame))?;
                match event {
                    transfer::ReceiveEvent::Closed => Ok(IssueEvent::Closed),
                    transfer::ReceiveEvent::Page(_) => unreachable!("close cannot yield page"),
                }
            }
            IssuedOwnedFrame::Page(frame) => {
                let expected_pool = self.permits.instance_id();
                if frame.pool_instance_id != expected_pool {
                    return Err(IssuedRxError::PermitPoolMismatch {
                        expected: expected_pool,
                        actual: frame.pool_instance_id,
                    });
                }
                match self.rx.accept(transfer::OwnedFrame::Page(frame.inner)) {
                    Ok(transfer::ReceiveEvent::Page(page)) => {
                        Ok(IssueEvent::Page(IssuedReceivedPage {
                            page: Some(page),
                            permits: self.permits,
                            permit_id: frame.permit_id,
                            released: false,
                        }))
                    }
                    Ok(transfer::ReceiveEvent::Closed) => unreachable!("page frame cannot close"),
                    Err(err) => {
                        if should_release_permit(&err) {
                            match self.permits.release(frame.permit_id) {
                                Ok(_) => Err(IssuedRxError::Rx(err)),
                                Err(permit) => {
                                    Err(IssuedRxError::RxAndPermitRelease { rx: err, permit })
                                }
                            }
                        } else {
                            Err(IssuedRxError::Rx(err))
                        }
                    }
                }
            }
        }
    }
}

impl IssuedReceivedPage {
    /// Return the in-page message kind.
    pub fn kind(&self) -> transfer::MessageKind {
        self.page
            .as_ref()
            .expect("received page must exist until release")
            .kind()
    }

    /// Return the in-page message flags.
    pub fn flags(&self) -> u16 {
        self.page
            .as_ref()
            .expect("received page must exist until release")
            .flags()
    }

    /// Return the detached page descriptor currently owned by this page.
    pub fn descriptor(&self) -> pool::PageDescriptor {
        self.page
            .as_ref()
            .expect("received page must exist until release")
            .descriptor()
    }

    /// Return the issuance permit id currently attached to this page.
    pub fn permit_id(&self) -> u32 {
        self.permit_id
    }

    /// Borrow the validated message payload bytes.
    pub fn payload(&self) -> &[u8] {
        self.page
            .as_ref()
            .expect("received page must exist until release")
            .payload()
    }

    /// Release both the detached page and its issuance permit.
    pub fn release(mut self) -> Result<(), IssuedRxError> {
        let page = self
            .page
            .take()
            .expect("received page must exist until release");
        let page_result = page.release();
        let permit_result = self.permits.release(self.permit_id);
        self.released = true;

        match (page_result, permit_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) => Err(IssuedRxError::Rx(err)),
            (Ok(()), Err(err)) => Err(IssuedRxError::PermitRelease(err)),
            (Err(rx), Err(permit)) => Err(IssuedRxError::RxAndPermitRelease { rx, permit }),
        }
    }
}

fn should_release_permit(err: &transfer::RxError) -> bool {
    match err {
        transfer::RxError::Busy
        | transfer::RxError::Closed
        | transfer::RxError::Access(_)
        | transfer::RxError::Release(_)
        | transfer::RxError::UnexpectedTransferId { .. } => false,
        transfer::RxError::InvalidPage(_) | transfer::RxError::Decode(_) => true,
    }
}

impl Drop for IssuedReceivedPage {
    fn drop(&mut self) {
        if !self.released {
            let page = self.page.take();
            drop(page);
            let _ = self.permits.release(self.permit_id);
            self.released = true;
        }
    }
}

impl import::OwnedPage for IssuedReceivedPage {
    fn kind(&self) -> transfer::MessageKind {
        Self::kind(self)
    }

    fn flags(&self) -> u16 {
        Self::flags(self)
    }

    fn payload(&self) -> &[u8] {
        Self::payload(self)
    }
}

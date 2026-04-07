use transfer::MessageKind;

/// One logical plan identity scoped to one executor/backend session epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FlowId {
    /// Monotonic backend/runtime epoch used to reject stale frames.
    pub session_epoch: u64,
    /// Stable plan identifier within one session epoch.
    pub plan_id: u64,
}

/// Open descriptor for one logical plan transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanOpen {
    /// Logical flow identity shared by sender and receiver roles.
    pub flow: FlowId,
    /// Expected in-page message kind for all plan pages in this flow.
    pub page_kind: MessageKind,
    /// Expected in-page message flags for all plan pages in this flow.
    pub page_flags: u16,
}

impl PlanOpen {
    /// Create a new logical plan-flow descriptor.
    pub fn new(flow: FlowId, page_kind: MessageKind, page_flags: u16) -> Self {
        Self {
            flow,
            page_kind,
            page_flags,
        }
    }
}

use std::collections::{HashMap, VecDeque};

pub type ScanId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CreditCell {
    pub slot_id: u16,
    pub block_idx: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchRequest {
    pub scan_id: ScanId,
    pub table_oid: u32,
    pub credit: CreditCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub quantum: usize,
    pub max_inflight_per_scan: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            quantum: 1,
            max_inflight_per_scan: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    DuplicateScan { scan_id: ScanId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseError {
    UnknownScan {
        scan_id: ScanId,
    },
    InflightUnderflow {
        scan_id: ScanId,
    },
    UnknownCredit {
        credit: CreditCell,
    },
    CreditOwnerMismatch {
        credit: CreditCell,
        expected_scan_id: ScanId,
        got_scan_id: ScanId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScanState {
    table_oid: u32,
    inflight: usize,
    deficit: usize,
    quantum: usize,
    eof: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetStats {
    pub scans: usize,
    pub active_scans: usize,
    pub free_credits: usize,
    pub leased_credits: usize,
}

#[derive(Debug, Default)]
pub struct BudgetScheduler {
    config: SchedulerConfig,
    scans: HashMap<ScanId, ScanState>,
    active: VecDeque<ScanId>,
    free_credits: VecDeque<CreditCell>,
    leased_credits: HashMap<CreditCell, ScanId>,
}

impl BudgetScheduler {
    pub fn new(cells: impl IntoIterator<Item = CreditCell>, mut config: SchedulerConfig) -> Self {
        if config.quantum == 0 {
            config.quantum = 1;
        }
        if config.max_inflight_per_scan == 0 {
            config.max_inflight_per_scan = 1;
        }

        Self {
            config,
            scans: HashMap::new(),
            active: VecDeque::new(),
            free_credits: cells.into_iter().collect(),
            leased_credits: HashMap::new(),
        }
    }

    pub fn with_slot_blocks(
        slot_count: usize,
        blocks_per_slot: usize,
        config: SchedulerConfig,
    ) -> Self {
        let slot_count_u16 =
            u16::try_from(slot_count).expect("slot_count must fit into u16 for protocol slot_id");
        let blocks_per_slot_u16 = u16::try_from(blocks_per_slot)
            .expect("blocks_per_slot must fit into u16 for protocol block_idx");
        let mut cells = Vec::with_capacity(slot_count.saturating_mul(blocks_per_slot));
        for slot_id in 0..slot_count_u16 {
            for block_idx in 0..blocks_per_slot_u16 {
                cells.push(CreditCell { slot_id, block_idx });
            }
        }
        Self::new(cells, config)
    }

    pub fn register_scan(&mut self, scan_id: ScanId, table_oid: u32) -> Result<(), RegisterError> {
        if self.scans.contains_key(&scan_id) {
            return Err(RegisterError::DuplicateScan { scan_id });
        }
        self.scans.insert(
            scan_id,
            ScanState {
                table_oid,
                inflight: 0,
                deficit: 0,
                quantum: self.config.quantum,
                eof: false,
            },
        );
        self.active.push_back(scan_id);
        Ok(())
    }

    pub fn mark_scan_eof(&mut self, scan_id: ScanId) -> bool {
        let mut remove = false;
        let Some(state) = self.scans.get_mut(&scan_id) else {
            return false;
        };
        state.eof = true;
        if state.inflight == 0 {
            remove = true;
        }
        self.active.retain(|id| *id != scan_id);
        if remove {
            self.scans.remove(&scan_id);
        }
        true
    }

    pub fn dispatch(&mut self, max_requests: usize) -> Vec<DispatchRequest> {
        if max_requests == 0 || self.active.is_empty() || self.free_credits.is_empty() {
            return Vec::new();
        }

        let cap = max_requests.min(self.free_credits.len());
        let mut out = Vec::with_capacity(cap);

        while out.len() < max_requests && !self.free_credits.is_empty() && !self.active.is_empty() {
            let scans_this_round = self.active.len();
            let mut progress = false;

            for _ in 0..scans_this_round {
                if out.len() >= max_requests || self.free_credits.is_empty() {
                    break;
                }

                let scan_id = self.active.pop_front().expect("active is non-empty");
                let mut requeue = false;
                let mut remove = false;
                let mut dispatched_for_scan = 0usize;

                if let Some(state) = self.scans.get_mut(&scan_id) {
                    if state.eof {
                        remove = state.inflight == 0;
                    } else {
                        requeue = true;
                        state.deficit = state.deficit.saturating_add(state.quantum);
                        while out.len() < max_requests
                            && !self.free_credits.is_empty()
                            && state.deficit > 0
                            && state.inflight < self.config.max_inflight_per_scan
                        {
                            let credit = self
                                .free_credits
                                .pop_front()
                                .expect("free_credits is non-empty");
                            state.deficit -= 1;
                            state.inflight += 1;
                            let prev = self.leased_credits.insert(credit, scan_id);
                            debug_assert!(prev.is_none(), "credit must not be leased twice");
                            out.push(DispatchRequest {
                                scan_id,
                                table_oid: state.table_oid,
                                credit,
                            });
                            dispatched_for_scan += 1;
                        }
                    }
                }

                if remove {
                    self.scans.remove(&scan_id);
                } else if requeue {
                    self.active.push_back(scan_id);
                }

                if dispatched_for_scan > 0 {
                    progress = true;
                }
            }

            if !progress {
                break;
            }
        }

        out
    }

    pub fn release(&mut self, scan_id: ScanId, credit: CreditCell) -> Result<(), ReleaseError> {
        if !self.scans.contains_key(&scan_id) {
            return Err(ReleaseError::UnknownScan { scan_id });
        }

        let Some(owner) = self.leased_credits.get(&credit).copied() else {
            return Err(ReleaseError::UnknownCredit { credit });
        };

        if owner != scan_id {
            return Err(ReleaseError::CreditOwnerMismatch {
                credit,
                expected_scan_id: owner,
                got_scan_id: scan_id,
            });
        }

        let state = self
            .scans
            .get_mut(&scan_id)
            .expect("scan existence checked above");
        if state.inflight == 0 {
            return Err(ReleaseError::InflightUnderflow { scan_id });
        }

        state.inflight -= 1;
        self.leased_credits
            .remove(&credit)
            .expect("leased credit must exist");
        self.free_credits.push_back(credit);

        if state.eof && state.inflight == 0 {
            self.scans.remove(&scan_id);
        }

        Ok(())
    }

    pub fn stats(&self) -> BudgetStats {
        BudgetStats {
            scans: self.scans.len(),
            active_scans: self.active.len(),
            free_credits: self.free_credits.len(),
            leased_credits: self.leased_credits.len(),
        }
    }

    pub fn scan_inflight(&self, scan_id: ScanId) -> Option<usize> {
        self.scans.get(&scan_id).map(|s| s.inflight)
    }

    pub fn has_scan(&self, scan_id: ScanId) -> bool {
        self.scans.contains_key(&scan_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BudgetScheduler, CreditCell, DispatchRequest, RegisterError, ReleaseError, SchedulerConfig,
    };

    fn cfg(quantum: usize, max_inflight_per_scan: usize) -> SchedulerConfig {
        SchedulerConfig {
            quantum,
            max_inflight_per_scan,
        }
    }

    #[test]
    fn allocates_and_releases_credits() {
        let mut sched = BudgetScheduler::with_slot_blocks(1, 2, cfg(1, 4));
        sched.register_scan(1, 42).expect("register scan");

        let req = sched.dispatch(8);
        assert_eq!(req.len(), 2);
        assert_eq!(sched.scan_inflight(1), Some(2));
        assert_eq!(sched.stats().free_credits, 0);

        sched
            .release(1, req[0].credit)
            .expect("release first leased credit");
        assert_eq!(sched.scan_inflight(1), Some(1));
        assert_eq!(sched.stats().free_credits, 1);

        let req2 = sched.dispatch(8);
        assert_eq!(req2.len(), 1);
        assert_eq!(sched.scan_inflight(1), Some(2));
    }

    #[test]
    fn round_robin_dispatch_is_fair_between_scans() {
        let mut sched = BudgetScheduler::with_slot_blocks(4, 1, cfg(1, 8));
        sched.register_scan(10, 100).expect("register scan 10");
        sched.register_scan(20, 200).expect("register scan 20");

        let req = sched.dispatch(4);
        let scan_ids: Vec<u64> = req.iter().map(|r| r.scan_id).collect();
        assert_eq!(scan_ids, vec![10, 20, 10, 20]);
    }

    #[test]
    fn respects_inflight_limit_per_scan() {
        let mut sched = BudgetScheduler::with_slot_blocks(4, 1, cfg(2, 1));
        sched.register_scan(1, 42).expect("register scan");

        let req = sched.dispatch(16);
        assert_eq!(req.len(), 1);
        assert_eq!(sched.scan_inflight(1), Some(1));
        assert_eq!(sched.stats().free_credits, 3);
    }

    #[test]
    fn release_rejects_wrong_owner_and_unknown_credit() {
        let mut sched = BudgetScheduler::with_slot_blocks(2, 1, cfg(1, 8));
        sched.register_scan(1, 10).expect("register scan 1");
        sched.register_scan(2, 20).expect("register scan 2");

        let req = sched.dispatch(1);
        assert_eq!(req.len(), 1);
        let credit = req[0].credit;
        let owner = req[0].scan_id;
        let other = if owner == 1 { 2 } else { 1 };

        let err = sched
            .release(other, credit)
            .expect_err("release by non-owner must fail");
        assert!(matches!(
            err,
            ReleaseError::CreditOwnerMismatch {
                expected_scan_id,
                got_scan_id,
                ..
            } if expected_scan_id == owner && got_scan_id == other
        ));

        sched
            .release(owner, credit)
            .expect("release by owner must succeed");
        let err = sched
            .release(owner, credit)
            .expect_err("double release must fail");
        assert!(matches!(err, ReleaseError::UnknownCredit { .. }));
    }

    #[test]
    fn eof_scan_stops_dispatch_and_is_collected_after_release() {
        let mut sched = BudgetScheduler::with_slot_blocks(2, 1, cfg(1, 8));
        sched.register_scan(1, 10).expect("register scan 1");
        sched.register_scan(2, 20).expect("register scan 2");

        let req = sched.dispatch(2);
        assert_eq!(req.len(), 2);
        let req_scan1 = req
            .iter()
            .find(|r| r.scan_id == 1)
            .copied()
            .expect("scan 1 request");
        let req_scan2 = req
            .iter()
            .find(|r| r.scan_id == 2)
            .copied()
            .expect("scan 2 request");

        assert!(sched.mark_scan_eof(1));
        assert!(sched.has_scan(1));
        assert_eq!(sched.scan_inflight(1), Some(1));

        sched
            .release(1, req_scan1.credit)
            .expect("release eof scan inflight");
        assert!(!sched.has_scan(1));

        sched
            .release(2, req_scan2.credit)
            .expect("release scan 2 inflight");
        let next = sched.dispatch(1);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].scan_id, 2);
    }

    #[test]
    fn duplicate_scan_registration_is_rejected() {
        let mut sched = BudgetScheduler::with_slot_blocks(1, 1, cfg(1, 1));
        sched.register_scan(7, 11).expect("first registration");
        let err = sched
            .register_scan(7, 11)
            .expect_err("duplicate registration must fail");
        assert_eq!(err, RegisterError::DuplicateScan { scan_id: 7 });
    }

    #[test]
    fn dispatch_returns_empty_when_no_resources() {
        let mut sched = BudgetScheduler::new([], cfg(1, 1));
        sched.register_scan(1, 1).expect("register scan");
        assert!(sched.dispatch(4).is_empty());
    }

    #[test]
    fn dispatch_request_contains_table_oid_and_credit() {
        let mut sched = BudgetScheduler::new(
            [CreditCell {
                slot_id: 5,
                block_idx: 9,
            }],
            cfg(1, 4),
        );
        sched.register_scan(77, 1234).expect("register scan");
        let req = sched.dispatch(1);
        assert_eq!(
            req,
            vec![DispatchRequest {
                scan_id: 77,
                table_oid: 1234,
                credit: CreditCell {
                    slot_id: 5,
                    block_idx: 9,
                },
            }]
        );
    }
}

use super::{ScanPageEnd, ScanProgress, ScanTailError, ScanTransition, TailPageEnd, TailProgress};

impl<Cut: Clone, Cursor: PartialEq> ScanProgress<Cut, Cursor> {
    /// The caller qualifies cut scope and owns its effect/conflict decisions.
    /// No fuzzy scan completion grants publication authority.
    pub fn advance_page(
        &self,
        end: ScanPageEnd<Cursor, Cut>,
    ) -> Result<ScanTransition<Cut, Cursor>, ScanTailError> {
        match end {
            ScanPageEnd::Continue(next) if Some(&next) != self.after.as_ref() => {
                Ok(ScanTransition::Scanning(ScanProgress {
                    low: self.low.clone(),
                    after: Some(next),
                }))
            }
            ScanPageEnd::Continue(_) => Err(ScanTailError::ProgressConflict),
            ScanPageEnd::Complete(high) => Ok(ScanTransition::CatchingUp(TailProgress {
                cursor: self.low.clone(),
                high,
            })),
        }
    }
}

impl<Cut: Clone + PartialEq> TailProgress<Cut> {
    pub fn advance_page(&self, end: TailPageEnd<Cut>) -> Result<Self, ScanTailError> {
        let cursor = match end {
            TailPageEnd::Continue(next) if next != self.cursor && next != self.high => next,
            TailPageEnd::Continue(_) => return Err(ScanTailError::InvalidBoundary),
            TailPageEnd::ReachedThrough => self.high.clone(),
        };
        Ok(Self {
            cursor,
            high: self.high.clone(),
        })
    }
}

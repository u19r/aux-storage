use crate::model::Slot;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum GsiQuerySpace {
    Primary,
    Alternate,
}

impl GsiQuerySpace {
    pub const ALL: [Self; 2] = [Self::Primary, Self::Alternate];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueryDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PartitionId {
    Left,
    Right,
}

impl PartitionId {
    pub const ALL: [Self; 2] = [Self::Left, Self::Right];

    #[must_use]
    pub const fn lower_bound(self) -> Slot {
        match self {
            Self::Left => 0,
            Self::Right => 2,
        }
    }

    #[must_use]
    pub const fn upper_bound(self) -> Slot {
        match self {
            Self::Left => 1,
            Self::Right => 3,
        }
    }

    #[must_use]
    pub const fn infer(lower_bound: Slot) -> Self {
        if lower_bound >= 2 {
            Self::Right
        } else {
            Self::Left
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueryTarget {
    Base,
    Gsi(GsiQuerySpace),
}

impl QueryTarget {
    pub const fn is_gsi(self) -> bool {
        matches!(self, Self::Gsi(_))
    }

    pub const fn gsi_query_space(self) -> Option<GsiQuerySpace> {
        match self {
            Self::Base => None,
            Self::Gsi(space) => Some(space),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRequest {
    pub lower_bound: Slot,
    pub upper_bound: Slot,
    pub start_exclusive: i8,
    pub limit: usize,
    pub byte_budget: usize,
    pub only_even: bool,
    pub direction: QueryDirection,
    pub target: QueryTarget,
    pub partition: PartitionId,
}

impl QueryRequest {
    #[must_use]
    pub fn is_valid(self) -> bool {
        if self.lower_bound > self.upper_bound {
            return false;
        }
        if self.limit == 0 || self.byte_budget == 0 {
            return false;
        }

        let partition_lower = i16::from(self.partition.lower_bound());
        let partition_upper = i16::from(self.partition.upper_bound());
        let lower_bound = i16::from(self.lower_bound);
        let upper_bound = i16::from(self.upper_bound);
        if lower_bound < partition_lower || upper_bound > partition_upper {
            return false;
        }

        match self.direction {
            QueryDirection::Forward => {
                let min_start = partition_lower - 1;
                let max_start = upper_bound;
                let start = i16::from(self.start_exclusive);
                start >= min_start && start <= max_start
            }
            QueryDirection::Reverse => {
                let min_start = lower_bound;
                let max_start = partition_upper + 1;
                let start = i16::from(self.start_exclusive);
                start >= min_start && start <= max_start
            }
        }
    }
}

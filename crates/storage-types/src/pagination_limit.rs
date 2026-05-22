use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationLimit {
    min: u32,
    default: u32,
    max: u32,
}

impl PaginationLimit {
    #[must_use]
    pub const fn new(default: u32, max: u32) -> Self {
        Self::with_min(1, default, max)
    }

    #[must_use]
    pub const fn with_min(min: u32, default: u32, max: u32) -> Self {
        Self { min, default, max }
    }

    #[must_use]
    pub const fn min_limit(&self) -> u32 {
        self.min
    }

    #[must_use]
    pub const fn default_limit(&self) -> u32 {
        self.default
    }

    #[must_use]
    pub const fn max_limit(&self) -> u32 {
        self.max
    }

    #[must_use]
    pub fn clamp(&self, requested: Option<u32>) -> u32 {
        let limit = requested.unwrap_or(self.default);
        limit.clamp(self.min, self.max)
    }

    #[must_use]
    pub fn clamp_usize(&self, requested: Option<usize>) -> usize {
        match requested {
            Some(value) => {
                let as_u32 = u32::try_from(value).unwrap_or(self.max);
                self.clamp(Some(as_u32)) as usize
            }
            None => self.clamp(None) as usize,
        }
    }

    pub fn validate(&self, requested: u32) -> Result<u32, PaginationLimitError> {
        if !(self.min..=self.max).contains(&requested) {
            return Err(PaginationLimitError::new(requested, self.min, self.max));
        }
        Ok(requested)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationLimitError {
    provided: u32,
    min: u32,
    max: u32,
}

impl PaginationLimitError {
    #[must_use]
    pub const fn new(provided: u32, min: u32, max: u32) -> Self {
        Self { provided, min, max }
    }

    #[must_use]
    pub const fn provided(&self) -> u32 {
        self.provided
    }

    #[must_use]
    pub const fn min_limit(&self) -> u32 {
        self.min
    }

    #[must_use]
    pub const fn max_limit(&self) -> u32 {
        self.max
    }
}

impl fmt::Display for PaginationLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "limit must be between {} and {} (got {})",
            self.min, self.max, self.provided
        )
    }
}

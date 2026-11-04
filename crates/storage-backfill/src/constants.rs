/// Keep exact source images below the native transaction envelope.
pub(crate) const GSI_HISTORY_READ_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::MIN.saturating_add(8 * 1024 * 1024 - 1);

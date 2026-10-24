use super::SystemStreamContractError;

pub(super) fn put_string(
    output: &mut Vec<u8>,
    value: &str,
) -> Result<(), SystemStreamContractError> {
    let length =
        u16::try_from(value.len()).map_err(|_| SystemStreamContractError::CheckpointTooLarge)?;
    output.extend(length.to_be_bytes());
    output.extend(value.as_bytes());
    Ok(())
}

pub(super) fn take_u8(payload: &[u8], cursor: &mut usize) -> Result<u8, SystemStreamContractError> {
    Ok(take_fixed::<1>(payload, cursor)?[0])
}

pub(super) fn take_u16(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u16, SystemStreamContractError> {
    Ok(u16::from_be_bytes(take_fixed(payload, cursor)?))
}

pub(super) fn take_u64(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u64, SystemStreamContractError> {
    Ok(u64::from_be_bytes(take_fixed(payload, cursor)?))
}

fn take_fixed<const N: usize>(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<[u8; N], SystemStreamContractError> {
    let bytes = payload
        .get(*cursor..cursor.saturating_add(N))
        .ok_or(SystemStreamContractError::MalformedCheckpoint)?;
    let bytes: [u8; N] = bytes
        .try_into()
        .map_err(|_| SystemStreamContractError::MalformedCheckpoint)?;
    *cursor += N;
    Ok(bytes)
}

pub(super) fn take_string(
    payload: &[u8],
    cursor: &mut usize,
    max_bytes: usize,
) -> Result<String, SystemStreamContractError> {
    let length = usize::from(take_u16(payload, cursor)?);
    if length == 0 || length > max_bytes {
        return Err(SystemStreamContractError::MalformedCheckpoint);
    }
    let bytes = payload
        .get(*cursor..cursor.saturating_add(length))
        .ok_or(SystemStreamContractError::MalformedCheckpoint)?;
    *cursor += length;
    String::from_utf8(bytes.to_vec()).map_err(|_| SystemStreamContractError::MalformedCheckpoint)
}

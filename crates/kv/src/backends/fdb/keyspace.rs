pub(super) fn prefix_bytes(prefix: Option<&Vec<u8>>, key: &[u8]) -> Vec<u8> {
    if let Some(prefix) = prefix {
        let mut composed = prefix.clone();
        composed.extend_from_slice(key);
        composed
    } else {
        key.to_vec()
    }
}

pub(super) fn strip_prefix<'a>(key: &'a [u8], prefix: Option<&Vec<u8>>) -> &'a [u8] {
    if let Some(prefix) = prefix
        && key.starts_with(prefix)
    {
        return &key[prefix.len()..];
    }
    key
}

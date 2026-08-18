use foundationdb::tuple::{Bytes, Element, pack};

pub(super) fn encode_prefix(prefix: Option<&[u8]>) -> Vec<u8> {
    let element = prefix.map_or(Element::Nil, |prefix| Element::Bytes(Bytes::from(prefix)));
    pack(&(element,))
}

pub(super) fn prefix_bytes(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut composed = Vec::with_capacity(prefix.len() + key.len());
    composed.extend_from_slice(prefix);
    composed.extend_from_slice(key);
    composed
}

pub(super) fn strip_prefix<'a>(key: &'a [u8], prefix: &[u8]) -> &'a [u8] {
    if key.starts_with(prefix) {
        return &key[prefix.len()..];
    }
    key
}

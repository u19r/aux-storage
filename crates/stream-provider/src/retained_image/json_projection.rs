//! Bounded JSON spelling projection above exact retained-byte verification.
use std::io::{self, BufRead, BufReader, Cursor, Read};

use super::numeric_token::NumericToken;
use crate::constants::{RETAINED_JSON_MAX_CONTAINERS, RETAINED_PAYLOAD_DECODE_CHUNK_BYTES};

pub(super) struct RetainedJsonProjection<'a, R> {
    input: BufReader<R>,
    pending: Cursor<Vec<u8>>,
    containers: Vec<Container>,
    remaining: &'a mut usize,
    maximum_string_bytes: usize,
}

enum Container {
    Object { is_key: bool, tag: Tag },
    Array { is_number_set: bool },
}

#[derive(Clone, Copy)]
enum Tag {
    Other,
    Number,
    NumberSet,
}

impl<'a, R: Read> RetainedJsonProjection<'a, R> {
    pub(super) fn new(input: R, maximum: &'a mut usize) -> Self {
        Self {
            input: BufReader::with_capacity(RETAINED_PAYLOAD_DECODE_CHUNK_BYTES, input),
            pending: Cursor::new(Vec::new()),
            containers: Vec::new(),
            remaining: maximum,
            maximum_string_bytes: usize::MAX,
        }
    }

    pub(super) fn for_item(input: R, maximum: &'a mut usize) -> Self {
        let mut projection = Self::new(input, maximum);
        projection.maximum_string_bytes =
            storage_types::MAX_ITEM_SIZE_BYTES.div_ceil(3) * 4 * 6 + 2;
        projection
    }

    pub(super) fn try_for_images(
        input: R,
        maximum: &'a mut usize,
        metadata_bytes: usize,
    ) -> io::Result<Self> {
        let mut projection = Self::for_item(input, maximum);
        let inline = metadata_bytes
            .checked_mul(6)
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(invalid_projection)?;
        projection.maximum_string_bytes = projection.maximum_string_bytes.max(inline);
        Ok(projection)
    }

    fn read_token(&mut self) -> io::Result<Option<Vec<u8>>> {
        let Some(byte) = self.read_projected_byte()? else {
            return Ok(None);
        };
        if byte == b'"' {
            return self.read_string().map(Some);
        }
        self.observe_delimiter(byte)?;
        Ok(Some(vec![byte]))
    }

    fn read_projected_byte(&mut self) -> io::Result<Option<u8>> {
        let mut byte = [0];
        if self.input.read(&mut byte)? == 0 {
            return Ok(None);
        }
        if !is_json_whitespace(byte[0]) {
            return Ok(Some(byte[0]));
        }
        // Preserve a separator: removing it could turn malformed `tru e` into
        // `true`.
        loop {
            let bytes = self.input.fill_buf()?;
            let count = bytes
                .iter()
                .take_while(|byte| is_json_whitespace(**byte))
                .count();
            if count == 0 {
                return Ok(Some(b' '));
            }
            self.input.consume(count);
        }
    }

    fn read_string(&mut self) -> io::Result<Vec<u8>> {
        if self.is_numeric_value() {
            let mut token = NumericToken::new(&mut self.input);
            let value = storage_types::project_dynamodb_number_from_bytes(&mut token)
                .map_err(|_| invalid_projection())?;
            token.require_complete()?;
            return serde_json::to_vec(&value).map_err(|_| invalid_projection());
        }
        let raw = self.read_quoted_string()?;
        let value: String = serde_json::from_slice(&raw).map_err(|_| invalid_projection())?;
        self.observe_key(&value);
        serde_json::to_vec(&value).map_err(|_| invalid_projection())
    }

    fn read_quoted_string(&mut self) -> io::Result<Vec<u8>> {
        // A JSON escape consumes at most six raw bytes per decoded UTF-8 byte.
        let maximum = (*self.remaining)
            .checked_mul(6)
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(invalid_projection)?
            .min(self.maximum_string_bytes);
        let mut raw = vec![b'"'];
        let mut escaped = false;
        loop {
            let mut byte = [0];
            self.input.read_exact(&mut byte)?;
            if raw.len() == maximum {
                return Err(invalid_projection());
            }
            raw.push(byte[0]);
            if byte[0] == b'"' && !escaped {
                return Ok(raw);
            }
            escaped = byte[0] == b'\\' && !escaped;
        }
    }

    fn is_numeric_value(&self) -> bool {
        matches!(
            self.containers.last(),
            Some(Container::Object {
                is_key: false,
                tag: Tag::Number
            }) | Some(Container::Array {
                is_number_set: true
            })
        )
    }

    fn observe_key(&mut self, value: &str) {
        if let Some(Container::Object { is_key, tag }) = self.containers.last_mut() {
            if !*is_key {
                return;
            }
            *tag = match value {
                "N" => Tag::Number,
                "NS" => Tag::NumberSet,
                _ => Tag::Other,
            };
            *is_key = false;
        }
    }

    fn observe_delimiter(&mut self, byte: u8) -> io::Result<()> {
        match byte {
            b'{' => self.push_container(Container::Object {
                is_key: true,
                tag: Tag::Other,
            })?,
            b'[' => {
                let is_number_set = matches!(
                    self.containers.last(),
                    Some(Container::Object {
                        is_key: false,
                        tag: Tag::NumberSet
                    })
                );
                self.push_container(Container::Array { is_number_set })?;
            }
            b'}' | b']' => self.close_container(byte)?,
            b',' => {
                if let Some(Container::Object { is_key, tag }) = self.containers.last_mut() {
                    *is_key = true;
                    *tag = Tag::Other;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn close_container(&mut self, byte: u8) -> io::Result<()> {
        if !matches!(
            (byte, self.containers.pop()),
            (b'}', Some(Container::Object { .. })) | (b']', Some(Container::Array { .. }))
        ) {
            return Err(invalid_projection());
        }
        Ok(())
    }

    fn push_container(&mut self, container: Container) -> io::Result<()> {
        if self.containers.len() == RETAINED_JSON_MAX_CONTAINERS {
            return Err(invalid_projection());
        }
        self.containers.push(container);
        Ok(())
    }
}

impl<R: Read> Read for RetainedJsonProjection<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let emitted = self.pending.read(output)?;
        if emitted != 0 {
            return Ok(emitted);
        }
        let Some(token) = self.read_token()? else {
            if !self.containers.is_empty() {
                return Err(invalid_projection());
            }
            return Ok(0);
        };
        *self.remaining = self
            .remaining
            .checked_sub(token.len())
            .ok_or_else(invalid_projection)?;
        self.pending = Cursor::new(token);
        self.pending.read(output)
    }
}

fn invalid_projection() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid retained JSON projection",
    )
}

fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\r' | b'\t')
}

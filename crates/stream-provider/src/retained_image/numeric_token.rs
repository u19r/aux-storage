//! Numeric JSON strings stream into the existing number grammar without padding
//! storage.
use std::io::{self, Read};

pub(super) struct NumericToken<'a, R> {
    input: &'a mut R,
    complete: bool,
    failure: Option<io::Error>,
}

impl<'a, R: Read> NumericToken<'a, R> {
    pub(super) fn new(input: &'a mut R) -> Self {
        Self {
            input,
            complete: false,
            failure: None,
        }
    }

    pub(super) fn require_complete(self) -> io::Result<()> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if !self.complete {
            return Err(invalid_token());
        }
        Ok(())
    }

    fn read_byte(&mut self) -> io::Result<u8> {
        let mut byte = [0];
        self.input.read_exact(&mut byte)?;
        Ok(byte[0])
    }

    fn read_escape(&mut self) -> io::Result<u8> {
        if self.read_byte()? != b'u' {
            return Err(invalid_token());
        }
        let mut value = 0u16;
        for _ in 0..4 {
            let digit = char::from(self.read_byte()?)
                .to_digit(16)
                .ok_or_else(invalid_token)?;
            value = (value << 4) | u16::try_from(digit).map_err(|_| invalid_token())?;
        }
        let byte = u8::try_from(value).map_err(|_| invalid_token())?;
        if !byte.is_ascii() {
            return Err(invalid_token());
        }
        Ok(byte)
    }

    fn read_digit(&mut self) -> io::Result<Option<u8>> {
        match self.read_byte()? {
            b'"' => {
                self.complete = true;
                Ok(None)
            }
            b'\\' => self.read_escape().map(Some),
            byte if byte >= 0x20 && byte.is_ascii() => Ok(Some(byte)),
            _ => Err(invalid_token()),
        }
    }
}

impl<R: Read> Iterator for NumericToken<'_, R> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        if self.complete || self.failure.is_some() {
            return None;
        }
        match self.read_digit() {
            Ok(byte) => byte,
            Err(error) => {
                self.failure = Some(error);
                None
            }
        }
    }
}

fn invalid_token() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "Invalid retained numeric token")
}

//! Errors from parsing a ReadSequence string template.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSequenceStringTemplateError {
    MissingInput,
    UnclosedInput,
    UnexpectedClosingBrace,
    InvalidInput,
}

impl std::fmt::Display for ReadSequenceStringTemplateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MissingInput => "template must contain at least one {input}",
            Self::UnclosedInput => "template contains an unclosed input",
            Self::UnexpectedClosingBrace => "template contains an unexpected closing brace",
            Self::InvalidInput => {
                "template input names must contain only ASCII letters, numbers, or _"
            }
        })
    }
}

impl std::error::Error for ReadSequenceStringTemplateError {}

pub trait ErrorContext<S, T, E> {
    fn context(self, context: impl Into<String>) -> Result<T, E>
    where Self: Sized;

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T, E>
    where Self: Sized {
        self.context(LazyContext(f))
    }
}

struct LazyContext<F>(F);

impl<F: FnOnce() -> String> From<LazyContext<F>> for String {
    fn from(value: LazyContext<F>) -> Self {
        (value.0)()
    }
}

pub trait ResultExt<T, E> {
    /// Log/map the error using the provided function.
    fn log_and_map<F, E2>(self, f: F) -> Result<T, E2>
    where F: FnOnce(&E) -> E2;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn log_and_map<F, E2>(self, f: F) -> Result<T, E2>
    where F: FnOnce(&E) -> E2 {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(f(&error)),
        }
    }
}

pub trait ResultOptionExt<T, E> {
    /// Convert Ok(None) into the supplied error.
    fn and_required(self, error: E) -> Result<T, E>;
}

impl<T, E> ResultOptionExt<T, E> for Result<Option<T>, E> {
    fn and_required(self, error: E) -> Result<T, E> {
        match self {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(error),
            Err(err) => Err(err),
        }
    }
}

pub trait WrappedError<I: std::fmt::Display> {
    fn to_enum(&self) -> &I;
    fn recursive_context(&self, context: Vec<String>) -> (&I, Vec<String>);
}

impl<I: std::fmt::Display> std::fmt::Display for dyn WrappedError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.to_enum()))
    }
}

/// Immutable frames shared by error values generated across crate boundaries.
/// The generated owners keep their frame private; this type is public only for
/// macro expansion in downstream crates.
#[doc(hidden)]
pub enum ErrorContextFrame<I> {
    Base(I),
    Context {
        error: std::sync::Arc<ErrorContextFrame<I>>,
        context: String,
    },
}

#[macro_export]
macro_rules! err_context {
    ($error_name:ident, $enum_name:ident) => {
        pub struct $error_name(std::sync::Arc<$crate::context::ErrorContextFrame<$enum_name>>);

        impl Clone for $error_name {
            fn clone(&self) -> Self {
                Self(std::sync::Arc::clone(&self.0))
            }
        }

        impl<U, E: Into<$error_name>> $crate::context::ErrorContext<$error_name, U, $error_name>
            for Result<U, E>
        where E: From<$error_name>
        {
            fn context(self, context: impl Into<String>) -> Result<U, $error_name> {
                self.map_err(|error| error.into().with_context(context))
            }
        }

        impl<T: Into<$enum_name>> From<T> for $error_name {
            fn from(value: T) -> Self {
                Self(std::sync::Arc::new(
                    $crate::context::ErrorContextFrame::Base(value.into()),
                ))
            }
        }

        impl std::error::Error for $error_name {}

        impl std::fmt::Display for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(self.as_ref(), f)
            }
        }

        impl $crate::context::WrappedError<$enum_name> for $error_name {
            fn to_enum(&self) -> &$enum_name {
                let mut frame = self.0.as_ref();
                loop {
                    match frame {
                        $crate::context::ErrorContextFrame::Base(error) => return error,
                        $crate::context::ErrorContextFrame::Context { error, .. } => {
                            frame = error.as_ref();
                        }
                    }
                }
            }

            fn recursive_context(&self, mut context: Vec<String>) -> (&$enum_name, Vec<String>) {
                let mut frame = self.0.as_ref();
                loop {
                    match frame {
                        $crate::context::ErrorContextFrame::Base(error) => return (error, context),
                        $crate::context::ErrorContextFrame::Context {
                            error,
                            context: message,
                        } => {
                            context.push(message.clone());
                            frame = error.as_ref();
                        }
                    }
                }
            }
        }

        impl std::fmt::Debug for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
                use std::error::Error;
                let (error, contexts) = <$error_name as $crate::context::WrappedError<
                    $enum_name,
                >>::recursive_context(self, Vec::new());
                write!(f, "{error}")?;
                if !contexts.is_empty() {
                    writeln!(f, "\n\nContext:")?;
                }
                for (index, context) in contexts.iter().enumerate() {
                    writeln!(f, "    {}: {context}", index + 1)?;
                }
                if let Some(source) = error.source() {
                    write!(f, "\nCaused by: {source:?}")?;
                }
                Ok(())
            }
        }

        impl $error_name {
            /// Add operation context without copying the original cause.
            #[must_use]
            pub fn with_context(self, context: impl Into<String>) -> Self {
                Self(std::sync::Arc::new(
                    $crate::context::ErrorContextFrame::Context {
                        error: self.0,
                        context: context.into(),
                    },
                ))
            }

            /// Preserve callers whose retry policy accepts only an unwrapped
            /// base failure, rather than failures carrying operation context.
            #[must_use]
            pub fn base_error(&self) -> Option<&$enum_name> {
                match self.0.as_ref() {
                    $crate::context::ErrorContextFrame::Base(error) => Some(error),
                    $crate::context::ErrorContextFrame::Context { .. } => None,
                }
            }

            #[must_use]
            pub fn into_enum(self) -> $enum_name {
                match std::sync::Arc::try_unwrap(self.0) {
                    Ok($crate::context::ErrorContextFrame::Base(error)) => error,
                    Ok($crate::context::ErrorContextFrame::Context { error, .. }) => {
                        Self(error).into_enum()
                    }
                    Err(shared) => Self(shared).as_ref().clone(),
                }
            }
        }

        impl AsRef<$enum_name> for $error_name {
            fn as_ref(&self) -> &$enum_name {
                <$error_name as $crate::context::WrappedError<$enum_name>>::to_enum(self)
            }
        }
    };
}

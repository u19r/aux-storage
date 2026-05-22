pub trait ErrorContext<S, T, E> {
    fn context(self, context: impl Into<String>) -> Result<T, E>
    where Self: Sized;

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T, E>
    where Self: Sized {
        self.context(f())
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

#[macro_export]
macro_rules! err_context {
    ($error_name:ident, $enum_name:ident) => {
        impl<U, E: Into<$error_name>> $crate::context::ErrorContext<$error_name, U, $error_name>
            for Result<U, E>
        where E: From<$error_name>
        {
            fn context(self, context: impl Into<String>) -> Result<U, $error_name> {
                match self {
                    Ok(t) => Ok(t),
                    Err(e) => Err($error_name::Context {
                        error: Box::new(e.into()),
                        context: context.into(),
                    }),
                }
            }
        }

        pub enum $error_name {
            Base($enum_name),
            Context {
                error: Box<$error_name>,
                context: String,
            },
        }

        impl<T: Into<$enum_name>> From<T> for $error_name {
            fn from(value: T) -> Self {
                $error_name::Base(value.into())
            }
        }

        impl std::error::Error for $error_name {}

        impl std::fmt::Display for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_fmt(format_args!("{}", self.as_ref()))
            }
        }

        impl $crate::context::WrappedError<$enum_name> for $error_name {
            fn to_enum(&self) -> &$enum_name {
                match self {
                    $error_name::Base(b) => b,
                    $error_name::Context { error, .. } => error.to_enum(),
                }
            }

            fn recursive_context(&self, mut context: Vec<String>) -> (&$enum_name, Vec<String>) {
                match self {
                    $error_name::Base(b) => (b, context),
                    $error_name::Context { error, context: c } => {
                        context.push(c.to_string());
                        error.recursive_context(context)
                    }
                }
            }
        }

        impl std::fmt::Debug for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
                use std::error::Error;
                let (err, ctx) =
                    <$error_name as $crate::context::WrappedError<$enum_name>>::recursive_context(
                        self,
                        Vec::new(),
                    );
                f.write_fmt(format_args!("{err}"))?;

                if !ctx.is_empty() {
                    f.write_fmt(format_args!("\n\nContext:\n"))?;
                }
                for (i, ctx) in ctx.iter().enumerate() {
                    f.write_fmt(format_args!("    {}: {ctx}\n", i + 1))?;
                }

                if let Some(source) = err.source() {
                    f.write_fmt(format_args!("\nCaused by: {source:?}"))?;
                }
                Ok(())
            }
        }

        impl AsRef<$enum_name> for $error_name {
            fn as_ref(&self) -> &$enum_name {
                match self {
                    $error_name::Base(b) => b,
                    $error_name::Context { error, .. } => error.as_ref().as_ref(),
                }
            }
        }
    };
}

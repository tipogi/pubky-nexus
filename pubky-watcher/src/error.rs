/// Possible error types of an event processor run.
#[derive(Debug)]
pub enum RunError<E> {
    Internal(E),
    Panicked,
    TimedOut,
}

impl<E> RunError<E> {
    pub fn is_panic(&self) -> bool {
        matches!(self, RunError::Panicked)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, RunError::TimedOut)
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RunError<E> {}

impl<E: std::fmt::Display> std::fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Internal(err) => write!(f, "Internal error: {err}"),
            RunError::Panicked => write!(f, "Execution panicked"),
            RunError::TimedOut => write!(f, "Execution timed out"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct WriteStatement<P> {
    pub(crate) sql: String,
    pub(crate) params: Vec<P>,
}

impl<P> WriteStatement<P> {
    pub(crate) fn new(sql: String, params: Vec<P>) -> Self {
        Self { sql, params }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WriteMaintenancePlan<P> {
    statements: Vec<WriteStatement<P>>,
}

impl<P> WriteMaintenancePlan<P> {
    #[allow(dead_code)]
    pub(crate) fn empty() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            statements: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn push(&mut self, statement: WriteStatement<P>) {
        self.statements.push(statement);
    }

    #[allow(dead_code)]
    pub(crate) fn statements(&self) -> &[WriteStatement<P>] {
        &self.statements
    }
}

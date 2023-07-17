use crate::{common, query_engine, KnownError};
use common::ModelKind;
use indoc::formatdoc;
use quaint::{error::ErrorKind, prelude::ConnectionInfo};

impl From<&quaint::error::DatabaseConstraint> for query_engine::DatabaseConstraint {
    fn from(other: &quaint::error::DatabaseConstraint) -> Self {
        match other {
            quaint::error::DatabaseConstraint::Fields(fields) => Self::Fields(fields.to_vec()),
            quaint::error::DatabaseConstraint::Index(index) => Self::Index(index.to_string()),
            quaint::error::DatabaseConstraint::ForeignKey => Self::ForeignKey,
            quaint::error::DatabaseConstraint::CannotParse => Self::CannotParse,
        }
    }
}

impl From<quaint::error::DatabaseConstraint> for query_engine::DatabaseConstraint {
    fn from(other: quaint::error::DatabaseConstraint) -> Self {
        match other {
            quaint::error::DatabaseConstraint::Fields(fields) => Self::Fields(fields.to_vec()),
            quaint::error::DatabaseConstraint::Index(index) => Self::Index(index),
            quaint::error::DatabaseConstraint::ForeignKey => Self::ForeignKey,
            quaint::error::DatabaseConstraint::CannotParse => Self::CannotParse,
        }
    }
}

pub fn invalid_connection_string_description(error_details: &str) -> String {
    let docs = r#"https://www.prisma.io/docs/reference/database-reference/connection-urls"#;

    let details = formatdoc! {r#"
            {} in database URL. Please refer to the documentation in {} for constructing a correct
            connection string. In some cases, certain characters must be escaped. Please
            check the string for any illegal characters."#, error_details, docs};

    details.replace('\n', " ")
}

pub fn render_quaint_error(kind: &ErrorKind, connection_info: &ConnectionInfo) -> Option<KnownError> {
    None
}

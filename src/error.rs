use crate::connector::ConnectorError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid Key hexadecimal representation: {0}")]
    ParseKey(String),

    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError)
}

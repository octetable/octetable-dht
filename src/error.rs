use crate::routing::RouterError;
use crate::transporter::TransporterError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid Key hexadecimal representation: {0}")]
    ParseKey(String),
    #[error("Transporter error: {0}")]
    Transporter(#[from] TransporterError),
    #[error("Router error: {0}")]
    Router(#[from] RouterError),
}

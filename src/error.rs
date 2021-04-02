#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid Key hexadecimal representation: {0}")]
    ParseKey(String),
}

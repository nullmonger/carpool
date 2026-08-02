#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum DedupError<E> {
    #[error("the fetch failed")]
    Load(#[source] E),
    #[error("the flight ended without an outcome")]
    Lost,
}

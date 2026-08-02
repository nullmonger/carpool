#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum DedupError<E> {
    #[error("fetch failed")]
    Load(#[source] E),
    #[error("fetch ended without a result")]
    Lost,
}

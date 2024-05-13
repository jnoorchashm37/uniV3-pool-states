pub use tracing::*;
pub use tracing_subscriber;
use tracing_subscriber::{
    filter::Directive, prelude::*, registry::LookupSpan, EnvFilter, Layer, Registry,
};

/// A boxed tracing [Layer].
pub type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

/// Initializes a new [Subscriber] based on the given layers.
pub fn init(layers: Vec<BoxedLayer<Registry>>) {
    tracing_subscriber::registry().with(layers).init();
}

/// Builds a new tracing layer that writes to stdout.
///
/// The events are filtered by `default_directive`, unless overridden by
/// `RUST_LOG`.
///
/// Colors can be disabled with `RUST_LOG_STYLE=never`, and event targets can be
/// displayed with `RUST_LOG_TARGET=1`.
pub fn stdout<S>(default_directive: Directive) -> BoxedLayer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let with_target = std::env::var("LOG_TARGET")
        .map(|val| val != "0")
        .unwrap_or(true);

    let filter = EnvFilter::builder()
        .with_default_directive(default_directive)
        .from_env_lossy();

    tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_target(with_target)
        .with_filter(filter)
        .boxed()
}

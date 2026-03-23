use std::path::Path;

use anyhow::Result;
use rdp_encode::EncoderType;

// Re-export config types from the shared crate.
pub use rdp_dbus::config::ServerConfig;

/// Load configuration from a TOML file.
///
/// If `path` is `None`, falls back to the default XDG location.
/// Returns the default configuration if the file does not exist.
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_config(path: Option<&Path>) -> Result<ServerConfig> {
    rdp_dbus::config::load(path)
}

/// Parse the `[encode].encoder` config string into an [`EncoderType`].
///
/// Returns `None` for `"auto"` (triggers auto-detection).
pub fn parse_encoder_type(cfg: &ServerConfig) -> Option<EncoderType> {
    let s = &cfg.encode.encoder;
    if s.eq_ignore_ascii_case("auto") {
        return None;
    }
    match s.parse() {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::warn!("{e}, falling back to auto-detect");
            None
        }
    }
}

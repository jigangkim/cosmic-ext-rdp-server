//! Screen capture abstraction for cosmic-ext-rdp-server.
//!
//! Provides screen capture via the XDG `ScreenCast` portal and `PipeWire`.
//!
//! Use [`start_capture`] for a high-level API that handles portal negotiation
//! and `PipeWire` stream setup.

pub mod audio_stream;
pub mod compositor;
pub mod frame;
pub mod pipewire_stream;
pub mod portal;
pub mod spa_meta;

pub use audio_stream::{AudioCaptureError, PwAudioStream};
pub use compositor::{bounding_box, FrameCompositor, MonitorInfo};
pub use frame::{
    AudioChunk, CaptureEvent, CapturedFrame, CursorBitmap, CursorInfo, DamageRect, PixelFormat,
};
pub use pipewire_stream::{PwError, PwStream};
pub use portal::{start_screencast, PortalError, PortalSession, PortalStream};

use ashpd::desktop::screencast::Screencast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Information about the captured desktop.
#[derive(Debug, Clone)]
pub struct DesktopInfo {
    /// Desktop width in pixels.
    pub width: u16,
    /// Desktop height in pixels.
    pub height: u16,
    /// `PipeWire` node ID (first stream).
    pub node_id: u32,
    /// Restore token for reconnecting to the same session.
    pub restore_token: Option<String>,
    /// X offset of the captured region in compositor coordinate space.
    ///
    /// Non-zero when a single monitor is selected that is not at the origin
    /// (e.g. the right monitor in a dual-monitor setup). Input coordinates
    /// from the RDP client must be translated by this offset before injection.
    pub x_offset: i32,
    /// Y offset of the captured region in compositor coordinate space.
    pub y_offset: i32,
}

/// Handle that keeps the capture session alive.
///
/// Dropping this stops the `PipeWire` stream and releases the portal session.
/// Must be kept alive for the duration of the capture.
pub struct CaptureHandle {
    _session: ashpd::desktop::Session<'static, Screencast<'static>>,
    _proxy: Screencast<'static>,
    _pw_streams: Vec<PwStream>,
    _compositor_task: Option<JoinHandle<()>>,
}

/// Start a screen capture session: portal negotiation + `PipeWire` stream.
///
/// Shows the system permission dialog if no valid `restore_token` is provided.
/// Returns a handle (must be kept alive), a receiver for captured frames,
/// and information about the captured desktop.
///
/// When `multi_monitor` is true and the portal returns multiple streams,
/// a [`FrameCompositor`] merges them into a single virtual desktop.
///
/// # Errors
///
/// Returns `CaptureError` if the portal session or `PipeWire` stream fails.
pub async fn start_capture(
    restore_token: Option<&str>,
    channel_capacity: usize,
    swap_colors: bool,
    multi_monitor: bool,
) -> Result<(CaptureHandle, mpsc::Receiver<CaptureEvent>, DesktopInfo), CaptureError> {
    let portal_session = start_screencast(restore_token, true, multi_monitor)
        .await
        .map_err(CaptureError::Portal)?;

    let PortalSession {
        session,
        proxy,
        streams,
        restore_token: token,
        pipewire_fd,
    } = portal_session;

    if streams.len() <= 1 {
        // Single monitor: no compositor needed.
        let stream = &streams[0];
        let info = DesktopInfo {
            width: stream
                .width
                .and_then(|w| u16::try_from(w).ok())
                .unwrap_or(1920),
            height: stream
                .height
                .and_then(|h| u16::try_from(h).ok())
                .unwrap_or(1080),
            node_id: stream.node_id,
            restore_token: token,
            x_offset: stream.x,
            y_offset: stream.y,
        };

        let (pw_stream, frame_rx) =
            PwStream::start(pipewire_fd, info.node_id, channel_capacity, swap_colors)
                .map_err(CaptureError::PipeWire)?;

        tracing::info!(
            width = info.width,
            height = info.height,
            node_id = info.node_id,
            "Screen capture session started (single monitor)"
        );

        let handle = CaptureHandle {
            _session: session,
            _proxy: proxy,
            _pw_streams: vec![pw_stream],
            _compositor_task: None,
        };

        return Ok((handle, frame_rx, info));
    }

    // Multi-monitor: create a PipeWire stream per monitor, then compose.
    tracing::info!(count = streams.len(), "Starting multi-monitor capture");

    let monitor_infos: Vec<MonitorInfo> = streams
        .iter()
        .map(|s| MonitorInfo {
            node_id: s.node_id,
            width: s
                .width
                .and_then(|w| u16::try_from(w).ok())
                .unwrap_or(1920),
            height: s
                .height
                .and_then(|h| u16::try_from(h).ok())
                .unwrap_or(1080),
            x: s.x,
            y: s.y,
        })
        .collect();

    let (canvas_width, canvas_height) = bounding_box(&monitor_infos);

    let mut pw_streams = Vec::with_capacity(streams.len());
    let mut monitor_rxs = Vec::with_capacity(streams.len());

    // Drop the initial FD — we open independent ones below.
    drop(pipewire_fd);

    for (i, stream) in streams.iter().enumerate() {
        // Each PipeWire stream needs an independent FD from the portal,
        // not a dup'd copy (dup'd FDs share the same socket buffer which
        // would corrupt messages between PipeWire cores).
        let fd = proxy
            .open_pipe_wire_remote(&session)
            .await
            .map_err(|e| CaptureError::Portal(PortalError::PipeWireRemote(e)))?;

        let (pw_stream, rx) =
            PwStream::start(fd, stream.node_id, channel_capacity, swap_colors)
                .map_err(CaptureError::PipeWire)?;

        tracing::info!(
            node_id = stream.node_id,
            x = stream.x,
            y = stream.y,
            width = ?stream.width,
            height = ?stream.height,
            "Started PipeWire stream for monitor {i}"
        );

        pw_streams.push(pw_stream);
        monitor_rxs.push(rx);
    }

    let (compositor, composed_rx) =
        FrameCompositor::new(&monitor_infos, monitor_rxs, channel_capacity);
    let compositor_task = tokio::spawn(compositor.run());

    let info = DesktopInfo {
        width: canvas_width,
        height: canvas_height,
        node_id: streams[0].node_id,
        restore_token: token,
        x_offset: 0,
        y_offset: 0,
    };

    tracing::info!(
        width = info.width,
        height = info.height,
        monitors = streams.len(),
        "Multi-monitor capture active (virtual desktop {}x{})",
        canvas_width,
        canvas_height,
    );

    let handle = CaptureHandle {
        _session: session,
        _proxy: proxy,
        _pw_streams: pw_streams,
        _compositor_task: Some(compositor_task),
    };

    Ok((handle, composed_rx, info))
}

#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("ScreenCast portal session failed")]
    Portal(#[source] PortalError),

    #[error("PipeWire stream failed")]
    PipeWire(#[source] PwError),
}

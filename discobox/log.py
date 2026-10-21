"""Per-device logging and the sync error counter."""

import logging
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("discobox")


# Errors a sync logs, counted per thread: every ERROR record on a discobox
# logger while this thread runs a sync (see sync_device), so an error that's
# only logged (a failed note update, a fan/PSU, …) still reaches the result's
# "errors" and the metrics, without each call site keeping its own counter.
_sync_error_tls = threading.local()


class _SyncErrorCounter(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if getattr(_sync_error_tls, "count", None) is not None:
            _sync_error_tls.count += 1


logger.addHandler(_SyncErrorCounter(level=logging.ERROR))


class _DeviceLogAdapter(logging.LoggerAdapter):
    """Prefixes every message with a device IP without creating a new named
    Logger per device — logging.getLogger() caches loggers for the life of the
    process, so one per IP (thousands, over a long-running server's uptime)
    never gets released. Delegates to the shared "discobox" logger, so level
    changes/handlers on it (e.g. /sync?debug=true's capture handler) still
    apply exactly as before."""
    def process(self, msg, kwargs):
        return f"{self.extra['ip']}  {msg}", kwargs

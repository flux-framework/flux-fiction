from __future__ import annotations

from contextlib import contextmanager
import itertools
import json
import logging
import os
import socket
from typing import Any


logger = logging.getLogger(__name__)


def _sanitize_attr_value(value: Any):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_sanitize_attr_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _sanitize_attr_value(v) for k, v in value.items()}
    return str(value)


class TelemetryClient:
    def __init__(self, socket_path: str | None, service_name: str, source: str):
        self.socket_path = socket_path or ""
        self.service_name = service_name
        self.source = source
        self.enabled = bool(self.socket_path)
        self._sock = None
        self._counter = itertools.count(1)

        if not self.enabled:
            return

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.setblocking(False)
            self._sock = sock
        except Exception:
            logger.exception("Failed to create telemetry socket client")
            self.enabled = False

    def close(self):
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def _send(self, payload: dict[str, Any]):
        if not self.enabled or self._sock is None:
            return
        payload.setdefault("service", self.service_name)
        payload.setdefault("source", self.source)
        payload.setdefault("pid", os.getpid())
        try:
            data = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
            self._sock.sendto(data, self.socket_path)
        except Exception:
            logger.debug("Telemetry send failed", exc_info=True)

    def start_span(self, name: str, **attrs: Any) -> str | None:
        if not self.enabled:
            return None
        span_id = "{}-{}-{}".format(self.source, os.getpid(), next(self._counter))
        payload = {
            "kind": "span_start",
            "span_id": span_id,
            "name": name,
        }
        if attrs:
            payload["attrs"] = {
                str(k): _sanitize_attr_value(v)
                for k, v in attrs.items()
                if v is not None
            }
        self._send(payload)
        return span_id

    def end_span(self, span_id: str | None, **attrs: Any):
        if not span_id:
            return
        payload = {
            "kind": "span_end",
            "span_id": span_id,
        }
        if attrs:
            payload["attrs"] = {
                str(k): _sanitize_attr_value(v)
                for k, v in attrs.items()
                if v is not None
            }
        self._send(payload)

    @contextmanager
    def span(self, name: str, **attrs: Any):
        span_id = self.start_span(name, **attrs)
        try:
            yield span_id
        finally:
            self.end_span(span_id)


from __future__ import annotations

import _framework_core


_REASON_PHRASES: dict[int, str] = {
    200: "OK",
    201: "Created",
    204: "No Content",
    301: "Moved Permanently",
    302: "Found",
    304: "Not Modified",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    405: "Method Not Allowed",
    413: "Content Too Large",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


class Response:
    """HTTP response object that buffers data and sends on finalize."""

    __slots__ = ("status", "_headers", "_multi_headers", "_body_parts", "_fd", "_finalized")

    status: int
    _headers: dict[str, str]
    _multi_headers: list[tuple[str, str]]
    _body_parts: list[bytes]
    _fd: int
    _finalized: bool

    def __init__(self, fd: int) -> None:
        self.status = 200
        self._headers = {}
        self._multi_headers = []
        self._body_parts = []
        self._fd = fd
        self._finalized = False

    def set_status(self, code: int) -> None:
        self.status = code

    def set_header(self, name: str, value: str) -> None:
        """Set a header (replaces existing). For single-value headers."""
        self._headers[name] = value

    def add_header(self, name: str, value: str) -> None:
        """Add a header (appends). For multi-value headers like Set-Cookie."""
        self._multi_headers.append((name, value))

    def write(self, data: bytes | str) -> None:
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._body_parts.append(data)

    def _finalize(self) -> None:
        if self._finalized:
            return
        self._finalized = True

        body = b"".join(self._body_parts)

        # Format headers as list of (bytes, bytes) for Zig.
        # Content-Length is NOT included here because writeResponse()
        # in Zig always auto-generates it from the body length.
        header_pairs: list[tuple[bytes, bytes]] = []
        for name, value in self._headers.items():
            if name == "Content-Length":
                continue
            header_pairs.append((name.encode("latin-1"), value.encode("latin-1")))
        # Append multi-value headers (Set-Cookie, etc.)
        for name, value in self._multi_headers:
            header_pairs.append((name.encode("latin-1"), value.encode("latin-1")))

        # Single Zig call: formats headers in arena + writev (zero-copy body)
        _framework_core.http_send_response(
            self._fd,
            self.status,
            header_pairs,
            body,
        )

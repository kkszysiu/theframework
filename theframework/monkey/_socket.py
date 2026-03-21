"""Monkey-patch ``socket`` to use io_uring cooperative I/O."""

from __future__ import annotations

import _socket
import errno as _errno_mod
import io as _io
import os as _os
import socket as _orig_socket_mod
import time as _time_mod
from collections.abc import Buffer

import _framework_core

# ---------------------------------------------------------------------------
# Hub detection
# ---------------------------------------------------------------------------


from theframework.monkey._state import get_fd as _get_fd, hub_is_running as _hub_is_running

# SOCK_NONBLOCK is Linux-specific (0x800).  We mask it from the type property
# when the socket is internally set non-blocking for io_uring.
_SOCK_NONBLOCK: int = getattr(_socket, "SOCK_NONBLOCK", 0x800)

# Poll event constants for green_poll_fd
_POLLIN: int = 0x001
_POLLOUT: int = 0x004

_socket_timeout = _orig_socket_mod.timeout


# ---------------------------------------------------------------------------
# Cooperative socket class
# ---------------------------------------------------------------------------


class socket(_socket.socket):
    """Drop-in replacement for :class:`socket.socket` that uses
    io_uring for all I/O when the hub is running."""

    __slots__ = ("_registered", "_timeout_value", "_io_refs", "_closed")

    def __init__(
        self,
        family: int = _socket.AF_INET,
        type: int = _socket.SOCK_STREAM,
        proto: int = 0,
        fileno: int | None = None,
    ) -> None:
        super().__init__(family, type, proto, fileno)
        self._registered: bool = False
        self._timeout_value: float | None = _orig_socket_mod.getdefaulttimeout()
        self._io_refs: int = 0
        self._closed: bool = False

    # -- type property (mask SOCK_NONBLOCK) -----------------------------------

    @property
    def type(self) -> int:
        t = super().type
        if self._registered:
            # We set the socket non-blocking internally for io_uring.
            # Mask out SOCK_NONBLOCK so callers see the original type.
            t &= ~_SOCK_NONBLOCK
        return t

    # -- registration helpers ------------------------------------------------

    def _ensure_registered(self) -> None:
        if not self._registered and _hub_is_running():
            # io_uring needs non-blocking fds
            super().setblocking(False)
            try:
                _framework_core.green_register_fd(self.fileno())
                self._registered = True
            except RuntimeError as e:
                import logging
                logging.getLogger("theframework.socket").error(
                    "green_register_fd failed for fd=%s: %s", self.fileno(), e
                )
                raise

    def _unregister(self) -> None:
        if self._registered:
            try:
                _framework_core.green_unregister_fd(self.fileno())
            except RuntimeError:
                pass
            self._registered = False

    # -- timeout bookkeeping -------------------------------------------------

    def settimeout(self, timeout: float | None) -> None:
        self._timeout_value = timeout
        if self._registered:
            # Already doing cooperative I/O — keep non-blocking
            super().setblocking(False)
        else:
            # Not yet registered — use native timeout so fallback works
            super().settimeout(timeout)

    def gettimeout(self) -> float | None:
        return self._timeout_value

    def setblocking(self, flag: bool) -> None:
        if flag:
            self.settimeout(None)
        else:
            self.settimeout(0.0)

    # -- timeout helpers -----------------------------------------------------

    def _get_deadline(self) -> float | None:
        """Return an absolute monotonic deadline, or None for no timeout."""
        t = self._timeout_value
        if t is None or t <= 0:
            return None
        return _time_mod.monotonic() + t

    def _check_deadline(self, deadline: float | None) -> None:
        """Raise socket.timeout if *deadline* has passed."""
        if deadline is not None and _time_mod.monotonic() >= deadline:
            raise _socket_timeout("timed out")

    # -- core I/O methods ----------------------------------------------------

    def recv(self, bufsize: int, flags: int = 0) -> bytes:
        if not _hub_is_running() or self._timeout_value == 0.0:
            return super().recv(bufsize, flags)
        self._ensure_registered()
        deadline = self._get_deadline()
        if flags:
            _framework_core.green_poll_fd(self.fileno(), _POLLIN)
            self._check_deadline(deadline)
            return super().recv(bufsize, flags)
        result = _framework_core.green_recv(self.fileno(), bufsize)
        self._check_deadline(deadline)
        return result

    def recv_into(self, buffer: Buffer, nbytes: int = 0, flags: int = 0) -> int:
        if not _hub_is_running() or self._timeout_value == 0.0:
            return super().recv_into(buffer, nbytes, flags)
        self._ensure_registered()
        deadline = self._get_deadline()
        if flags:
            _framework_core.green_poll_fd(self.fileno(), _POLLIN)
            self._check_deadline(deadline)
            return super().recv_into(buffer, nbytes, flags)
        size = nbytes if nbytes else memoryview(buffer).nbytes
        data = _framework_core.green_recv(self.fileno(), size)
        self._check_deadline(deadline)
        n = len(data)
        memoryview(buffer)[:n] = data
        return n

    def recvfrom(self, bufsize: int, flags: int = 0) -> tuple[bytes, object]:
        if not _hub_is_running() or self._timeout_value == 0.0:
            return super().recvfrom(bufsize, flags)
        self._ensure_registered()
        deadline = self._get_deadline()
        _framework_core.green_poll_fd(self.fileno(), _POLLIN)
        self._check_deadline(deadline)
        return super().recvfrom(bufsize, flags)

    def send(self, data: Buffer, flags: int = 0) -> int:
        if not _hub_is_running() or self._timeout_value == 0.0:
            return super().send(data, flags)
        self._ensure_registered()
        deadline = self._get_deadline()
        if flags:
            _framework_core.green_poll_fd(self.fileno(), _POLLOUT)
            self._check_deadline(deadline)
            return super().send(data, flags)
        bdata = bytes(data) if not isinstance(data, bytes) else data
        result = _framework_core.green_send(self.fileno(), bdata)
        self._check_deadline(deadline)
        return result

    def sendall(self, data: Buffer, flags: int = 0) -> None:
        if not _hub_is_running() or self._timeout_value == 0.0:
            super().sendall(data, flags)
            return
        self._ensure_registered()
        deadline = self._get_deadline()
        if flags:
            bdata = bytes(data) if not isinstance(data, bytes) else data
            sent = 0
            total = len(bdata)
            while sent < total:
                _framework_core.green_poll_fd(self.fileno(), _POLLOUT)
                self._check_deadline(deadline)
                sent += super().send(bdata[sent:], flags)
            return
        bdata = bytes(data) if not isinstance(data, bytes) else data
        _framework_core.green_send(self.fileno(), bdata)
        self._check_deadline(deadline)

    def sendto(self, data: Buffer, *args: object) -> int:
        if not _hub_is_running() or self._timeout_value == 0.0:
            result: int = super().sendto(data, *args)  # type: ignore[call-overload]
            return result
        self._ensure_registered()
        deadline = self._get_deadline()
        _framework_core.green_poll_fd(self.fileno(), _POLLOUT)
        self._check_deadline(deadline)
        result = super().sendto(data, *args)  # type: ignore[call-overload]
        return result

    def recvfrom_into(self, buffer: Buffer, nbytes: int = 0, flags: int = 0) -> tuple[int, object]:
        if not _hub_is_running() or self._timeout_value == 0.0:
            return super().recvfrom_into(buffer, nbytes, flags)
        self._ensure_registered()
        deadline = self._get_deadline()
        _framework_core.green_poll_fd(self.fileno(), _POLLIN)
        self._check_deadline(deadline)
        return super().recvfrom_into(buffer, nbytes, flags)

    # -- connect / accept ----------------------------------------------------

    def connect(self, address: tuple[object, ...] | str | Buffer) -> None:
        if not _hub_is_running() or self._timeout_value == 0.0:
            super().connect(address)
            return

        self._ensure_registered()
        deadline = self._get_deadline()

        if isinstance(address, (str, bytes, bytearray, memoryview)):
            # Unix socket path — poll for writability then connect
            _framework_core.green_poll_fd(self.fileno(), _POLLOUT)
            self._check_deadline(deadline)
            err = super().connect_ex(address)
            if err and err != _errno_mod.EINPROGRESS and err != _errno_mod.EISCONN:
                raise OSError(err, _os.strerror(err))
            if err == _errno_mod.EINPROGRESS:
                _framework_core.green_poll_fd(self.fileno(), _POLLOUT)
                self._check_deadline(deadline)
                err = self.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                if err:
                    raise OSError(err, _os.strerror(err))
            return

        # TCP: resolve address (handles hostnames)
        if not isinstance(address, tuple):
            super().connect(address)
            return
        host = str(address[0])
        port = int(str(address[1]))
        infos = _orig_socket_mod.getaddrinfo(host, port, self.family, self.type, self.proto)
        if not infos:
            raise OSError("getaddrinfo failed for %s:%s" % (host, port))

        last_err: OSError | None = None
        for _af, _socktype, _proto, _canonname, sa in infos:
            ip = str(sa[0])
            try:
                if _af == _socket.AF_INET6:
                    # Zig greenConnectFd only handles IPv4 — use poll-then-connect
                    err = super().connect_ex(sa)
                    if err == _errno_mod.EINPROGRESS:
                        _framework_core.green_poll_fd(self.fileno(), _POLLOUT)
                        self._check_deadline(deadline)
                        err = self.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                    if err and err != _errno_mod.EISCONN:
                        raise OSError(err, _os.strerror(err))
                else:
                    _framework_core.green_connect_fd(self.fileno(), ip, int(sa[1]))
                self._check_deadline(deadline)
                return
            except OSError as e:
                last_err = e
        if last_err is not None:
            raise last_err

    def connect_ex(self, address: tuple[object, ...] | str | Buffer) -> int:
        try:
            self.connect(address)
            return 0
        except OSError as e:
            return e.errno if e.errno is not None else _errno_mod.ECONNREFUSED

    def accept(self) -> tuple[socket, object]:
        if not _hub_is_running() or self._timeout_value == 0.0:
            raw_fd_int, raw_addr = super()._accept()  # type: ignore[misc]
            new_sock = socket(self.family, self.type, self.proto, fileno=raw_fd_int)
            return new_sock, raw_addr

        self._ensure_registered()
        deadline = self._get_deadline()
        new_fd = _framework_core.green_accept(self.fileno())
        self._check_deadline(deadline)

        new_sock = socket(self.family, self.type, self.proto, fileno=new_fd)
        new_sock._registered = True
        try:
            peer_addr: object = new_sock.getpeername()
        except OSError:
            if self.family == _socket.AF_INET6:
                peer_addr = ("::", 0, 0, 0)
            elif self.family == _socket.AF_UNIX:
                peer_addr = b""
            else:
                peer_addr = ("0.0.0.0", 0)
        return new_sock, peer_addr

    # -- makefile / context manager ------------------------------------------

    def makefile(
        self,
        mode: str = "r",
        buffering: int | None = None,
        *,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> _io.RawIOBase | _io.BufferedIOBase | _io.TextIOBase:
        """Return an I/O stream connected to the socket."""
        if not set(mode) <= {"r", "w", "b"}:
            raise ValueError("invalid mode %r (only r, w, b allowed)" % (mode,))
        writing = "w" in mode
        reading = "r" in mode or not writing
        binary = "b" in mode
        rawmode = ""
        if reading:
            rawmode += "r"
        if writing:
            rawmode += "w"
        raw = _orig_socket_mod.SocketIO(self, rawmode)  # type: ignore[arg-type]
        self._io_refs += 1
        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = _io.DEFAULT_BUFFER_SIZE
        if buffering == 0:
            if not binary:
                raise ValueError("unbuffered streams must be binary")
            return raw
        buf: _io.BufferedIOBase
        if reading and writing:
            buf = _io.BufferedRWPair(raw, raw, buffering)
        elif reading:
            buf = _io.BufferedReader(raw, buffering)
        else:
            buf = _io.BufferedWriter(raw, buffering)
        if binary:
            return buf
        text = _io.TextIOWrapper(buf, encoding, errors, newline)  # type: ignore[type-var]
        text.mode = mode  # type: ignore[misc]
        return text

    def _decref_socketios(self) -> None:
        if self._io_refs > 0:
            self._io_refs -= 1
        if self._closed:
            self.close()

    def __enter__(self) -> socket:
        return self

    def __exit__(self, *args: object) -> None:
        if not self._closed:
            self.close()

    # -- close / detach ------------------------------------------------------

    def close(self) -> None:
        self._closed = True
        if self._io_refs < 1:
            if self._registered:
                self._registered = False
                try:
                    # green_close cancels any pending io_uring operations
                    # on this fd (waking blocked greenlets with errors)
                    # and then closes the fd.
                    _framework_core.green_close(self.fileno())
                except RuntimeError, OSError:
                    # If green_close fails (e.g., hub not running),
                    # fall back to regular close.
                    super().close()
            else:
                super().close()

    def detach(self) -> int:
        self._unregister()
        return super().detach()

    def dup(self) -> socket:
        fd = _os.dup(self.fileno())
        new_sock = socket(self.family, self.type, self.proto, fileno=fd)
        new_sock.settimeout(self.gettimeout())
        return new_sock


# ---------------------------------------------------------------------------
# Module-level socket functions
# ---------------------------------------------------------------------------


_GLOBAL_DEFAULT_TIMEOUT = object()


def create_connection(
    address: tuple[str, int],
    timeout: object = _GLOBAL_DEFAULT_TIMEOUT,
    source_address: tuple[str, int] | None = None,
) -> socket:
    """Cooperative version of socket.create_connection."""
    host, port = address
    err: OSError | None = None
    for res in _orig_socket_mod.getaddrinfo(host, port, 0, _socket.SOCK_STREAM):
        af, socktype, proto, _canonname, sa = res
        sock: socket | None = None
        try:
            sock = socket(af, socktype, proto)
            if timeout is not _GLOBAL_DEFAULT_TIMEOUT:
                sock.settimeout(timeout)  # type: ignore[arg-type]
            if source_address:
                sock.bind(source_address)
            sock.connect(sa)
            return sock
        except OSError as exc:
            err = exc
            if sock is not None:
                sock.close()
    if err is not None:
        raise err
    raise OSError("getaddrinfo returns an empty list")


def socketpair(
    family: int = _socket.AF_UNIX,
    type: int = _socket.SOCK_STREAM,
    proto: int = 0,
) -> tuple[socket, socket]:
    """Cooperative socketpair."""
    a, b = _orig_socket_mod.socketpair(family, type, proto)
    sa = socket(family, type, proto, fileno=a.fileno())
    a.detach()
    sb = socket(family, type, proto, fileno=b.fileno())
    b.detach()
    return sa, sb


def fromfd(fd: int, family: int, type: int, proto: int = 0) -> socket:
    """Cooperative fromfd."""
    return socket(family, type, proto, fileno=_os.dup(fd))

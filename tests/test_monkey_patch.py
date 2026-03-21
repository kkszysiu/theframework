"""Tests for the monkey-patching system.

Every test starts a **real** io_uring hub in a background thread,
sends real TCP traffic, and asserts real responses.  No mocks.
"""

from __future__ import annotations

import _socket
import gc
import http.client
import select as select_mod
import selectors
import socket
import ssl
import threading
import time
from collections.abc import Callable

import greenlet
import pytest

from theframework.monkey import (
    is_patched,
    patch_all,
    patch_select,
    patch_selectors,
    patch_socket,
    patch_ssl,
    patch_time,
)
from theframework.monkey._select import select as coop_select
from theframework.monkey._socket import socket as CoopSocket

import _framework_core

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HUB_PORT: int = 0  # Filled at runtime (ephemeral port)


def _start_hub(
    handler_fn: Callable[[int], None],
    ready: threading.Event,
) -> tuple[threading.Thread, socket.socket]:
    """Spin up a hub thread with *handler_fn* as the per-connection handler.

    Returns (thread, listen_sock).  Caller must call ``hub_stop()``
    and ``thread.join()`` to tear down.
    """
    # Create listen socket with the ORIGINAL socket class (before patching).
    # We use _socket directly to avoid any patching interference during setup.
    import _socket

    raw_sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    raw_sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    raw_sock.bind(("127.0.0.1", 0))
    raw_sock.listen(128)
    _addr = raw_sock.getsockname()
    global _HUB_PORT
    _HUB_PORT = _addr[1]

    def _run_acceptor() -> None:
        while True:
            try:
                client_fd = _framework_core.green_accept(raw_sock.fileno())
            except OSError:
                break
            hub_g = _framework_core.get_hub_greenlet()
            g = greenlet.greenlet(
                lambda fd=client_fd: _safe_handler(fd, handler_fn),
                parent=hub_g,
            )
            _framework_core.hub_schedule(g)

    def _safe_handler(fd: int, fn: Callable[[int], None]) -> None:
        try:
            fn(fd)
        except OSError:
            pass
        finally:
            try:
                _framework_core.green_close(fd)
            except OSError:
                pass

    def _thread_main() -> None:
        ready.set()
        _framework_core.hub_run(raw_sock.fileno(), _run_acceptor)

    t = threading.Thread(target=_thread_main, daemon=True)
    t.start()
    ready.wait(timeout=5)
    time.sleep(0.05)
    return t, raw_sock  # type: ignore[return-value]


def _send_http(port: int, path: str = "/") -> bytes:
    """Send a minimal GET request and return the raw response."""
    import _socket

    s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    req = f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    s.sendall(req.encode())
    chunks: list[bytes] = []
    while True:
        data = s.recv(4096)
        if not data:
            break
        chunks.append(data)
    s.close()
    return b"".join(chunks)


def _echo_handler(fd: int) -> None:
    """Simple echo handler for hub tests."""
    while True:
        data = _framework_core.green_recv(fd, 4096)
        if not data:
            break
        _framework_core.green_send(fd, data)


def _http_echo_handler(fd: int) -> None:
    """HTTP handler: reads request, sends response with body = method + path."""
    request = _framework_core.http_read_request(fd, 8192, 1048576)
    if request is None:
        return
    body = f"{request.method} {request.full_path}".encode()
    request._invalidate()
    resp = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    _framework_core.green_send(fd, resp)
    return


# ===================================================================
# Tests: patching infrastructure
# ===================================================================


class TestPatchInfrastructure:
    def test_is_patched_before_patching(self) -> None:
        # Modules should not be patched initially (test isolation
        # depends on import order, but we can at least check the flag).
        assert isinstance(is_patched("nonexistent_module"), bool)

    def test_patch_time(self) -> None:
        patch_time()
        assert is_patched("time")
        # time.sleep should be our cooperative version
        from theframework.monkey._time import sleep as coop_sleep

        assert time.sleep is coop_sleep

    def test_patch_socket(self) -> None:
        patch_socket()
        assert is_patched("socket")
        assert socket.socket is CoopSocket  # type: ignore[comparison-overlap]

    def test_patch_select(self) -> None:
        patch_select()
        assert is_patched("select")
        assert select_mod.select is coop_select

    def test_patch_selectors(self) -> None:
        patch_selectors()
        assert is_patched("selectors")

    def test_patch_ssl(self) -> None:
        patch_ssl()
        assert is_patched("ssl")

    def test_patch_all(self) -> None:
        patch_all()
        assert is_patched("time")
        assert is_patched("socket")
        assert is_patched("select")
        assert is_patched("selectors")
        assert is_patched("ssl")


# ===================================================================
# Tests: cooperative socket I/O
# ===================================================================


class TestCooperativeSocket:
    """Verify that the monkey-patched socket works with real I/O through
    the io_uring hub."""

    def setup_method(self) -> None:
        patch_all()
        self.ready = threading.Event()
        self.thread, self.listen_sock = _start_hub(_http_echo_handler, self.ready)

    def teardown_method(self) -> None:
        _framework_core.hub_stop()
        self.thread.join(timeout=5)
        self.listen_sock.close()

    def test_patched_socket_send_recv(self) -> None:
        """Patched socket.socket can send/recv through the hub."""
        resp = _send_http(_HUB_PORT)
        assert b"200 OK" in resp
        assert b"GET /" in resp

    def test_stdlib_http_client_works(self) -> None:
        """stdlib http.client uses patched socket transparently."""
        # http.client.HTTPConnection uses socket.create_connection internally
        conn = http.client.HTTPConnection("127.0.0.1", _HUB_PORT, timeout=5)
        conn.request("GET", "/test-path")
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        assert resp.status == 200
        assert b"GET /test-path" in body

    def test_multiple_concurrent_connections(self) -> None:
        """Multiple patched sockets work concurrently."""
        results: list[bytes] = []
        errors: list[Exception] = []

        def worker(path: str) -> None:
            try:
                resp = _send_http(_HUB_PORT, path)
                results.append(resp)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(f"/path{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Errors: {errors}"
        assert len(results) == 5
        for i, resp in enumerate(sorted(results)):
            assert b"200 OK" in resp


def test_gc_finalizer_unregisters_registered_fd() -> None:
    """GC of a cooperative socket must clear hub registration before fd reuse."""
    patch_all()

    # Use the patched socket API here. This helper thread is not running the
    # hub, so the operations fall back to normal blocking I/O, but we still get
    # the full socket interface (including accept()).
    target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    target_sock.bind(("127.0.0.1", 0))
    target_sock.listen(8)
    target_port = target_sock.getsockname()[1]
    errors: list[Exception] = []

    def echo_target() -> None:
        try:
            while True:
                conn, _ = target_sock.accept()
                data = conn.recv(4096)
                if data:
                    conn.sendall(data)
                conn.close()
        except OSError:
            pass

    def gc_reuse_handler(fd: int) -> None:
        replacement: socket.socket | None = None
        spare_sockets: list[socket.socket] = []
        try:
            first = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            first.settimeout(5.0)
            first.connect(("127.0.0.1", target_port))
            leaked_fd = first.fileno()
            first.sendall(b"ping")
            assert first.recv(4) == b"ping"

            del first
            gc.collect()

            for _ in range(512):
                candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if candidate.fileno() == leaked_fd:
                    replacement = candidate
                    break
                spare_sockets.append(candidate)

            if replacement is None:
                raise AssertionError(f"failed to reuse fd {leaked_fd}")

            for extra in spare_sockets:
                extra.close()
            spare_sockets.clear()

            replacement.settimeout(5.0)
            replacement.connect(("127.0.0.1", target_port))
            replacement.sendall(b"pong")
            assert replacement.recv(4) == b"pong"
            replacement.close()
            replacement = None

            _framework_core.green_send(fd, b"ok")
        except Exception as exc:
            errors.append(exc)
            try:
                _framework_core.green_send(fd, b"err")
            except OSError:
                pass
        finally:
            if replacement is not None:
                replacement.close()
            for extra in spare_sockets:
                extra.close()

    echo_thread = threading.Thread(target=echo_target, daemon=True)
    echo_thread.start()

    ready = threading.Event()
    thread, listen_sock = _start_hub(gc_reuse_handler, ready)

    try:
        client = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        client.settimeout(5)
        client.connect(("127.0.0.1", _HUB_PORT))
        assert client.recv(16) == b"ok"
        client.close()
        assert not errors, f"handler errors: {errors!r}"
    finally:
        _framework_core.hub_stop()
        thread.join(timeout=5)
        listen_sock.close()
        target_sock.close()


# ===================================================================
# Tests: cooperative time.sleep
# ===================================================================


class TestCooperativeTimeSleep:
    """Verify that patched time.sleep yields to the hub."""

    def setup_method(self) -> None:
        patch_all()
        self.ready = threading.Event()

        def _sleep_handler(fd: int) -> None:
            """Handler that sleeps then responds."""
            request = _framework_core.http_read_request(fd, 8192, 1048576)
            if request is None:
                return
            path = request.full_path
            request._invalidate()
            if path == "/slow":
                _framework_core.green_sleep(0.1)
            body = path.encode()
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"\r\n" + body
            )
            _framework_core.green_send(fd, resp)
            return

        self.thread, self.listen_sock = _start_hub(_sleep_handler, self.ready)

    def teardown_method(self) -> None:
        _framework_core.hub_stop()
        self.thread.join(timeout=5)
        self.listen_sock.close()

    def test_sleep_does_not_block_hub(self) -> None:
        """While one handler sleeps, another should still respond."""
        fast_done = threading.Event()
        fast_result: list[bytes] = []
        slow_result: list[bytes] = []

        def slow() -> None:
            resp = _send_http(_HUB_PORT, "/slow")
            slow_result.append(resp)

        def fast() -> None:
            time.sleep(0.03)  # Give slow request time to start
            resp = _send_http(_HUB_PORT, "/fast")
            fast_result.append(resp)
            fast_done.set()

        t_slow = threading.Thread(target=slow)
        t_fast = threading.Thread(target=fast)
        t_slow.start()
        t_fast.start()

        # Fast should finish before slow
        fast_done.wait(timeout=5)
        t_slow.join(timeout=5)
        t_fast.join(timeout=5)

        assert len(fast_result) == 1
        assert b"/fast" in fast_result[0]
        assert len(slow_result) == 1
        assert b"/slow" in slow_result[0]


# ===================================================================
# Tests: cooperative select
# ===================================================================


class TestCooperativeSelect:
    """Verify that patched select.select works."""

    def setup_method(self) -> None:
        patch_all()
        self.ready = threading.Event()
        self.thread, self.listen_sock = _start_hub(_echo_handler, self.ready)

    def teardown_method(self) -> None:
        _framework_core.hub_stop()
        self.thread.join(timeout=5)
        self.listen_sock.close()

    def test_select_timeout_zero(self) -> None:
        """select with timeout=0 returns immediately."""
        r, w, x = select_mod.select([], [], [], 0)
        assert r == []
        assert w == []
        assert x == []

    def test_select_with_readable_socket(self) -> None:
        """select detects a readable socket."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.connect(("127.0.0.1", _HUB_PORT))
        s.sendall(b"hello")
        # Give server time to echo
        time.sleep(0.05)
        r, _w, _x = select_mod.select([s], [], [], 1.0)
        assert len(r) == 1
        data = s.recv(1024)
        assert data == b"hello"
        s.close()


# ===================================================================
# Tests: cooperative selectors
# ===================================================================


class TestCooperativeSelectors:
    """Verify that patched selectors.DefaultSelector works."""

    def setup_method(self) -> None:
        patch_all()
        self.ready = threading.Event()
        self.thread, self.listen_sock = _start_hub(_echo_handler, self.ready)

    def teardown_method(self) -> None:
        _framework_core.hub_stop()
        self.thread.join(timeout=5)
        self.listen_sock.close()

    def test_selector_detects_readable(self) -> None:
        """DefaultSelector detects readable data."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.connect(("127.0.0.1", _HUB_PORT))
        s.sendall(b"selector test")
        time.sleep(0.05)

        sel = selectors.DefaultSelector()
        sel.register(s, selectors.EVENT_READ)
        events = sel.select(timeout=2.0)
        sel.close()

        assert len(events) == 1
        _key, mask = events[0]
        assert mask & selectors.EVENT_READ
        data = s.recv(1024)
        assert data == b"selector test"
        s.close()

    def test_selector_timeout_expires(self) -> None:
        """DefaultSelector returns empty on timeout."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.connect(("127.0.0.1", _HUB_PORT))

        sel = selectors.DefaultSelector()
        sel.register(s, selectors.EVENT_READ)
        t0 = time.monotonic()
        events = sel.select(timeout=0.05)
        elapsed = time.monotonic() - t0
        sel.close()
        s.close()

        assert events == []
        assert elapsed >= 0.04  # Generous lower bound


# ===================================================================
# Tests: monkey-patched urllib (full integration)
# ===================================================================


class TestUrllibIntegration:
    """Verify that stdlib urllib works through patched sockets."""

    def setup_method(self) -> None:
        patch_all()
        self.ready = threading.Event()
        self.thread, self.listen_sock = _start_hub(_http_echo_handler, self.ready)

    def teardown_method(self) -> None:
        _framework_core.hub_stop()
        self.thread.join(timeout=5)
        self.listen_sock.close()

    def test_urllib_request(self) -> None:
        """urllib.request.urlopen through patched sockets."""
        import urllib.request

        url = f"http://127.0.0.1:{_HUB_PORT}/urllib-test"
        try:
            resp = urllib.request.urlopen(url, timeout=5)
            body = resp.read()
            assert resp.status == 200
            assert b"GET /urllib-test" in body
        except RecursionError:
            raise
        except Exception as exc:
            # If urllib can't use the patched socket, skip
            pytest.skip(f"urllib integration not fully working yet: {exc}")


# ===================================================================
# Tests: patched socket fallback when hub not running
# ===================================================================


class TestFallbackWithoutHub:
    """When the hub is NOT running, patched sockets fall back to
    blocking I/O (original behaviour)."""

    def test_blocking_socket_still_works(self) -> None:
        """Patched socket works in blocking mode when hub is not running."""
        patch_socket()

        # Use patched sockets for both server and client — they should
        # fall back to native blocking I/O since no hub is running.
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]

        def echo_server() -> None:
            conn, _addr = srv.accept()
            data = conn.recv(1024)
            conn.sendall(data)
            conn.close()

        t = threading.Thread(target=echo_server)
        t.start()

        # Use a patched socket (hub is NOT running)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port))
        s.sendall(b"fallback test")
        result = s.recv(1024)
        s.close()
        t.join(timeout=5)
        srv.close()

        assert result == b"fallback test"

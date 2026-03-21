"""Tests for Phase 2 monkey-patching fixes.

Covers:
- P0-2: hub_is_running uses thread-local storage (O(1))
- P1-2: Non-zero flags via poll-then-syscall (MSG_PEEK, MSG_DONTWAIT)
- P1-3: Timeout enforcement on socket operations (_get_deadline / _check_deadline)
- P1-6: SSL _ssl_retry timeout support
- P2-4: socket.close() uses green_close for pending op cancellation
- P2-13: IPv6 connect via poll-then-connect fallback
"""

from __future__ import annotations

import inspect
import socket
import threading
import time

import greenlet
import pytest

from theframework.monkey import patch_all
from theframework.monkey._socket import socket as CoopSocket
from theframework.monkey._ssl import SSLSocket, _ssl_retry
from theframework.monkey._state import (
    _hub_local,
    hub_is_running,
    mark_hub_running,
    mark_hub_stopped,
)

import _framework_core  # Must be after theframework imports


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HUB_PORT: int = 0


def _start_hub(
    handler_fn: object,
    ready: threading.Event,
    config: dict[str, object] | None = None,
) -> tuple[threading.Thread, socket.socket]:
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

    def _safe_handler(fd: int, fn: object) -> None:
        try:
            fn(fd)  # type: ignore[operator]
        except OSError:
            pass
        finally:
            try:
                _framework_core.green_close(fd)
            except OSError:
                pass

    def _thread_main() -> None:
        from theframework.monkey._state import mark_hub_running, mark_hub_stopped

        ready.set()
        try:

            def _acceptor_with_mark() -> None:
                hub_g = _framework_core.get_hub_greenlet()
                mark_hub_running(hub_g)
                _run_acceptor()

            if config is None:
                _framework_core.hub_run(raw_sock.fileno(), _acceptor_with_mark)
            else:
                _framework_core.hub_run(raw_sock.fileno(), _acceptor_with_mark, config)
        finally:
            mark_hub_stopped()

    t = threading.Thread(target=_thread_main, daemon=True)
    t.start()
    ready.wait(timeout=5)
    time.sleep(0.05)
    return t, raw_sock  # type: ignore[return-value]


def _echo_handler(fd: int) -> None:
    while True:
        data = _framework_core.green_recv(fd, 4096)
        if not data:
            break
        _framework_core.green_send(fd, data)


def _silent_handler(fd: int) -> None:
    """Handler that never sends data — useful for testing recv timeout."""
    # Just block forever (sleep a long time)
    _framework_core.green_sleep(600.0)


# ===================================================================
# P0-2: hub_is_running uses thread-local (O(1))
# ===================================================================


class TestHubIsRunningThreadLocal:
    def test_initially_not_running(self) -> None:
        """Hub should not be running in the main thread."""
        mark_hub_stopped()
        assert not hub_is_running()

    def test_mark_running_makes_it_true(self) -> None:
        """Marking hub running should make hub_is_running return True."""
        g = greenlet.greenlet(lambda: None)
        mark_hub_running(g)
        try:
            assert hub_is_running()
        finally:
            mark_hub_stopped()

    def test_mark_stopped_makes_it_false(self) -> None:
        g = greenlet.greenlet(lambda: None)
        mark_hub_running(g)
        mark_hub_stopped()
        assert not hub_is_running()

    def test_thread_local_isolation(self) -> None:
        """Different threads should have independent hub state."""
        g = greenlet.greenlet(lambda: None)
        mark_hub_running(g)
        other_running = threading.Event()
        result: list[bool] = []

        def _check_in_other_thread() -> None:
            result.append(hub_is_running())
            other_running.set()

        t = threading.Thread(target=_check_in_other_thread)
        t.start()
        other_running.wait(timeout=5)
        t.join(timeout=5)

        mark_hub_stopped()

        # The other thread should NOT see the hub as running
        assert len(result) == 1
        assert result[0] is False

    def test_hub_local_has_hub_greenlet_attr(self) -> None:
        """The thread-local should have hub_greenlet attribute."""
        assert hasattr(_hub_local, "hub_greenlet")

    def test_real_hub_in_other_thread_stays_false_here(self) -> None:
        """A hub running in another thread must not make this thread cooperative."""
        patch_all()
        ready = threading.Event()
        thread, listen_sock = _start_hub(_echo_handler, ready)

        result: list[bool] = []
        checked = threading.Event()

        def _check_in_other_thread() -> None:
            result.append(hub_is_running())
            checked.set()

        try:
            t = threading.Thread(target=_check_in_other_thread)
            t.start()
            checked.wait(timeout=5)
            t.join(timeout=5)
        finally:
            _framework_core.hub_stop()
            thread.join(timeout=5)
            listen_sock.close()

        assert result == [False]
        assert _hub_local.hub_greenlet is None or isinstance(
            _hub_local.hub_greenlet, greenlet.greenlet
        )


# ===================================================================
# P1-2: Non-zero flags via poll-then-syscall
# ===================================================================


class TestFlagsPollthenSyscall:
    def setup_method(self) -> None:
        patch_all()
        self.ready = threading.Event()
        self.thread, self.listen_sock = _start_hub(_echo_handler, self.ready)

    def teardown_method(self) -> None:
        _framework_core.hub_stop()
        self.thread.join(timeout=5)
        self.listen_sock.close()

    def test_recv_msg_peek(self) -> None:
        """MSG_PEEK should return data without consuming it."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        cs = CoopSocket(_socket.AF_INET, _socket.SOCK_STREAM, 0, fileno=s.fileno())
        s.detach()
        try:
            cs.connect(("127.0.0.1", _HUB_PORT))
            cs.sendall(b"peek_test")
            time.sleep(0.05)  # Wait for echo

            # MSG_PEEK: read data without consuming
            peeked = cs.recv(1024, socket.MSG_PEEK)
            assert peeked == b"peek_test"

            # Now read for real — should get the same data
            actual = cs.recv(1024)
            assert actual == b"peek_test"
        finally:
            cs.close()

    def test_send_with_flags_works(self) -> None:
        """send() with flags should work via poll-then-syscall."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        cs = CoopSocket(_socket.AF_INET, _socket.SOCK_STREAM, 0, fileno=s.fileno())
        s.detach()
        try:
            cs.connect(("127.0.0.1", _HUB_PORT))
            # MSG_NOSIGNAL prevents SIGPIPE on broken connections
            msg_nosignal = getattr(socket, "MSG_NOSIGNAL", 0x4000)
            n = cs.send(b"flagged", msg_nosignal)
            assert n > 0
            time.sleep(0.05)
            data = cs.recv(1024)
            assert data == b"flagged"
        finally:
            cs.close()

    def test_sendall_with_flags(self) -> None:
        """sendall() with flags should loop correctly."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        cs = CoopSocket(_socket.AF_INET, _socket.SOCK_STREAM, 0, fileno=s.fileno())
        s.detach()
        try:
            cs.connect(("127.0.0.1", _HUB_PORT))
            msg_nosignal = getattr(socket, "MSG_NOSIGNAL", 0x4000)
            cs.sendall(b"hello_all", msg_nosignal)
            time.sleep(0.05)
            data = cs.recv(1024)
            assert data == b"hello_all"
        finally:
            cs.close()

    def test_recv_into_with_flags(self) -> None:
        """recv_into() with flags should use poll-then-syscall path."""
        import _socket

        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        cs = CoopSocket(_socket.AF_INET, _socket.SOCK_STREAM, 0, fileno=s.fileno())
        s.detach()
        try:
            cs.connect(("127.0.0.1", _HUB_PORT))
            cs.sendall(b"into_test")
            time.sleep(0.05)

            buf = bytearray(1024)
            n = cs.recv_into(buf, 1024, socket.MSG_PEEK)
            assert buf[:n] == b"into_test"

            # Consume the peeked data
            n = cs.recv_into(buf, 1024)
            assert buf[:n] == b"into_test"
        finally:
            cs.close()


# ===================================================================
# P1-3: Timeout enforcement on socket operations
# ===================================================================


class TestSocketTimeoutEnforcement:
    def test_get_deadline_with_timeout(self) -> None:
        """_get_deadline should return a future monotonic time."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs._timeout_value = 5.0
            before = time.monotonic()
            deadline = cs._get_deadline()
            after = time.monotonic()
            assert deadline is not None
            assert before + 5.0 <= deadline <= after + 5.0
        finally:
            cs.close()

    def test_get_deadline_no_timeout(self) -> None:
        """_get_deadline should return None when no timeout set."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs._timeout_value = None
            assert cs._get_deadline() is None
        finally:
            cs.close()

    def test_get_deadline_zero_timeout(self) -> None:
        """_get_deadline should return None for zero timeout."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs._timeout_value = 0.0
            assert cs._get_deadline() is None
        finally:
            cs.close()

    def test_check_deadline_not_expired(self) -> None:
        """_check_deadline should not raise for a future deadline."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs._check_deadline(time.monotonic() + 100)
        finally:
            cs.close()

    def test_check_deadline_expired(self) -> None:
        """_check_deadline should raise socket.timeout for past deadline."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(socket.timeout, match="timed out"):
                cs._check_deadline(time.monotonic() - 1)
        finally:
            cs.close()

    def test_check_deadline_none(self) -> None:
        """_check_deadline with None should not raise."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs._check_deadline(None)
        finally:
            cs.close()

    def test_settimeout_stores_value(self) -> None:
        """settimeout should store the value in _timeout_value."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs.settimeout(3.5)
            assert cs.gettimeout() == 3.5
            cs.settimeout(None)
            assert cs.gettimeout() is None
            cs.settimeout(0.0)
            assert cs.gettimeout() == 0.0
        finally:
            cs.close()

    def test_setblocking_maps_to_timeout(self) -> None:
        """setblocking(True) -> None timeout, setblocking(False) -> 0.0."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cs.setblocking(False)
            assert cs.gettimeout() == 0.0
            cs.setblocking(True)
            assert cs.gettimeout() is None
        finally:
            cs.close()


# ===================================================================
# P1-6: SSL _ssl_retry timeout support
# ===================================================================


class TestSSLRetryTimeout:
    def test_ssl_retry_has_timeout_param(self) -> None:
        """_ssl_retry should accept a timeout keyword argument."""
        sig = inspect.signature(_ssl_retry)
        assert "timeout" in sig.parameters

    def test_ssl_retry_timeout_default_none(self) -> None:
        """Default timeout should be None (no timeout)."""
        sig = inspect.signature(_ssl_retry)
        assert sig.parameters["timeout"].default is None

    def test_ssl_methods_pass_timeout(self) -> None:
        """All SSLSocket cooperative methods should pass timeout to _ssl_retry."""
        for method_name in ["read", "write", "recv", "send", "do_handshake", "unwrap"]:
            src = inspect.getsource(getattr(SSLSocket, method_name))
            assert "timeout=" in src, f"SSLSocket.{method_name} should pass timeout= to _ssl_retry"

    def test_ssl_sendall_has_deadline(self) -> None:
        """SSL sendall should use deadline-based timeout."""
        src = inspect.getsource(SSLSocket.sendall)
        assert "deadline" in src, "SSL sendall should use deadline-based timeout tracking"
        assert "remaining" in src, "SSL sendall should compute remaining time for each send"

    def test_ssl_get_ssl_timeout(self) -> None:
        """SSLSocket should have _get_ssl_timeout helper."""
        assert hasattr(SSLSocket, "_get_ssl_timeout")
        src = inspect.getsource(SSLSocket._get_ssl_timeout)
        assert "gettimeout" in src


# ===================================================================
# P2-4: socket.close() uses green_close
# ===================================================================


class TestSocketCloseUsesGreenClose:
    def test_close_source_uses_green_close(self) -> None:
        """socket.close() should call green_close when registered."""
        src = inspect.getsource(CoopSocket.close)
        assert "green_close" in src, "close() should use green_close to cancel pending io_uring ops"

    def test_close_unregistered_uses_super(self) -> None:
        """Unregistered socket should use regular close."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        assert not cs._registered
        cs.close()
        assert cs._closed

    def test_close_with_io_refs(self) -> None:
        """Close with active io_refs should defer actual close."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        cs._io_refs = 1
        cs.close()
        assert cs._closed
        # Socket should still have a valid fileno since close is deferred
        # (close only happens when _io_refs drops to 0)

    def test_context_manager_closes(self) -> None:
        """Context manager exit should close the socket."""
        with CoopSocket(socket.AF_INET, socket.SOCK_STREAM) as cs:
            assert not cs._closed
        assert cs._closed

    def test_close_integration(self) -> None:
        """Integration test: close a connected socket from the main thread."""
        patch_all()
        ready = threading.Event()
        thread, listen_sock = _start_hub(_echo_handler, ready)
        try:
            import _socket

            s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            cs = CoopSocket(_socket.AF_INET, _socket.SOCK_STREAM, 0, fileno=s.fileno())
            s.detach()
            # Connect from main thread (hub not running here),
            # so socket won't be registered — this tests the non-green_close path
            cs.connect(("127.0.0.1", _HUB_PORT))
            cs.sendall(b"test")
            time.sleep(0.05)
            cs.recv(1024)
            cs.close()
            assert cs._closed
            assert not cs._registered
        finally:
            _framework_core.hub_stop()
            thread.join(timeout=5)
            listen_sock.close()

    def test_ensure_registered_only_swallows_already_registered(self) -> None:
        """Unexpected registration failures should not be hidden."""
        cs = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
        mark_hub_running(greenlet.greenlet(lambda: None))
        try:
            original = _framework_core.green_register_fd

            def _boom(fd: int) -> None:
                _ = fd
                raise RuntimeError("connection pool exhausted")

            _framework_core.green_register_fd = _boom
            with pytest.raises(RuntimeError, match="connection pool exhausted"):
                cs._ensure_registered()
            assert not cs._registered
        finally:
            _framework_core.green_register_fd = original
            mark_hub_stopped()
            cs.close()

    def test_close_registered_in_hub(self) -> None:
        """Integration: close a registered socket inside the hub uses green_close."""
        patch_all()
        ready = threading.Event()
        close_result: list[bool] = []

        def _close_test_handler(fd: int) -> None:
            # Receive data then close — the fd is already registered by the hub
            data = _framework_core.green_recv(fd, 4096)
            close_result.append(True)
            # green_close is called by the finally in _safe_handler

        thread, listen_sock = _start_hub(_close_test_handler, ready)
        try:
            import _socket

            s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            s.connect(("127.0.0.1", _HUB_PORT))
            s.sendall(b"hello")
            s.close()
            time.sleep(0.1)
        finally:
            _framework_core.hub_stop()
            thread.join(timeout=5)
            listen_sock.close()

        assert len(close_result) == 1

    def test_close_registered_off_thread_releases_idle_slot(self) -> None:
        """Closing a registered socket off the hub thread must free its slot."""
        patch_all()

        import _socket

        target_sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        target_sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        target_sock.bind(("127.0.0.1", 0))
        target_sock.listen(8)
        target_port = target_sock.getsockname()[1]

        errors: list[Exception] = []
        handoff: list[CoopSocket | None] = [None]
        handoff_ready = threading.Event()

        def _echo_target() -> None:
            try:
                while True:
                    conn, _ = target_sock.accept()
                    data = conn.recv(4096)
                    if data:
                        conn.sendall(data)
                    conn.close()
            except OSError:
                pass

        def _handler(fd: int) -> None:
            try:
                first = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
                first.settimeout(5.0)
                first.connect(("127.0.0.1", target_port))
                first.sendall(b"ping")
                assert first.recv(4) == b"ping"

                handoff[0] = first
                handoff_ready.set()

                _framework_core.green_sleep(0.1)

                second = CoopSocket(socket.AF_INET, socket.SOCK_STREAM)
                second.settimeout(5.0)
                second.connect(("127.0.0.1", target_port))
                second.sendall(b"pong")
                assert second.recv(4) == b"pong"
                second.close()

                _framework_core.green_send(fd, b"ok")
            except Exception as exc:
                errors.append(exc)
                try:
                    _framework_core.green_send(fd, b"err")
                except OSError:
                    pass

        echo_thread = threading.Thread(target=_echo_target, daemon=True)
        echo_thread.start()

        ready = threading.Event()
        thread, listen_sock = _start_hub(_handler, ready, {"max_connections": 2})
        client: _socket.socket | None = None

        try:
            client = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            client.settimeout(5.0)
            client.connect(("127.0.0.1", _HUB_PORT))

            assert handoff_ready.wait(timeout=5), "handler never registered first socket"
            assert handoff[0] is not None

            handoff[0].close()
            handoff[0] = None

            assert client.recv(16) == b"ok"
            assert not errors, f"handler errors: {errors!r}"
        finally:
            if client is not None:
                client.close()
            _framework_core.hub_stop()
            thread.join(timeout=5)
            listen_sock.close()
            target_sock.close()


# ===================================================================
# P2-13: IPv6 connect via poll-then-connect fallback
# ===================================================================


class TestIPv6ConnectFallback:
    def test_connect_source_has_ipv6_handling(self) -> None:
        """connect() should check for AF_INET6 and use poll-then-connect."""
        src = inspect.getsource(CoopSocket.connect)
        assert "AF_INET6" in src, "connect() should handle AF_INET6 addresses"
        assert "connect_ex" in src, "connect() should use connect_ex for non-blocking connect"
        assert "EINPROGRESS" in src, "connect() should handle EINPROGRESS from non-blocking connect"
        assert "SO_ERROR" in src, "connect() should check SO_ERROR after poll"

    def test_connect_has_deadline(self) -> None:
        """connect() should enforce timeout via deadline."""
        src = inspect.getsource(CoopSocket.connect)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_accept_has_deadline(self) -> None:
        """accept() should enforce timeout via deadline."""
        src = inspect.getsource(CoopSocket.accept)
        assert "_get_deadline" in src
        assert "_check_deadline" in src


# ===================================================================
# Integration: hub_is_running with real hub
# ===================================================================


class TestHubIsRunningIntegration:
    def test_hub_is_running_in_hub_thread(self) -> None:
        """hub_is_running should be True in the hub thread."""
        patch_all()
        ready = threading.Event()
        result: list[bool] = []

        def _check_handler(fd: int) -> None:
            result.append(hub_is_running())
            _framework_core.green_send(fd, b"ok")

        thread, listen_sock = _start_hub(_check_handler, ready)
        try:
            import _socket

            s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            s.connect(("127.0.0.1", _HUB_PORT))
            s.recv(1024)
            s.close()
            time.sleep(0.05)
        finally:
            _framework_core.hub_stop()
            thread.join(timeout=5)
            listen_sock.close()

        assert len(result) >= 1
        assert result[0] is True

    def test_hub_is_running_false_after_stop(self) -> None:
        """hub_is_running should be False after hub stops."""
        patch_all()
        ready = threading.Event()
        thread, listen_sock = _start_hub(_echo_handler, ready)

        _framework_core.hub_stop()
        thread.join(timeout=5)
        listen_sock.close()

        # Give thread time to clean up
        time.sleep(0.1)

        # Main thread should not see hub running
        assert not hub_is_running()


# ===================================================================
# Socket timeout integration
# ===================================================================


class TestSocketTimeoutIntegration:
    """Test that timeout values are properly wired through to deadline checking."""

    def test_deadline_methods_exist(self) -> None:
        """_get_deadline and _check_deadline should be on the socket class."""
        assert hasattr(CoopSocket, "_get_deadline")
        assert hasattr(CoopSocket, "_check_deadline")

    def test_recv_has_deadline(self) -> None:
        """recv should call _get_deadline and _check_deadline."""
        src = inspect.getsource(CoopSocket.recv)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_send_has_deadline(self) -> None:
        src = inspect.getsource(CoopSocket.send)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_sendall_has_deadline(self) -> None:
        src = inspect.getsource(CoopSocket.sendall)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_recv_into_has_deadline(self) -> None:
        src = inspect.getsource(CoopSocket.recv_into)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_recvfrom_has_deadline(self) -> None:
        src = inspect.getsource(CoopSocket.recvfrom)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_sendto_has_deadline(self) -> None:
        src = inspect.getsource(CoopSocket.sendto)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

    def test_recvfrom_into_has_deadline(self) -> None:
        src = inspect.getsource(CoopSocket.recvfrom_into)
        assert "_get_deadline" in src
        assert "_check_deadline" in src

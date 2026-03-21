from __future__ import annotations

import errno
import os
import resource
import select
import signal
import socket
import sys
import threading
import time
import traceback
from collections import defaultdict
from collections.abc import Callable

import greenlet

import _framework_core

from theframework.request import Request
from theframework.response import Response

HandlerFunc = Callable[[Request, Response], None]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CRASH_WINDOW: float = 60.0
MAX_CRASHES_IN_WINDOW: int = 5
SHUTDOWN_TIMEOUT: float = 3.0  # Short timeout for quick shutdown
_DEFAULT_GREENLET_STACK_SIZE: int = 0  # 0 = no override

# ---------------------------------------------------------------------------
# Module-level state (for signal handler communication)
# ---------------------------------------------------------------------------

_shutting_down: bool = False


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _apply_greenlet_stack_config(config: dict[str, int] | None) -> None:
    """Apply greenlet_stack_size from config.

    Greenlets share the OS thread's C stack. Deep call chains (e.g. Django
    middleware + ORM + template rendering) can exhaust the default 8 MiB stack.
    This function increases the stack limit for the current process and sets
    the default stack size for new threads.
    """
    if config is None:
        return
    stack_size = config.get("greenlet_stack_size", _DEFAULT_GREENLET_STACK_SIZE)
    if not stack_size or stack_size <= 0:
        return

    # 1. Increase the soft RLIMIT_STACK so new threads get a larger stack.
    #    On Linux this also allows the main thread's stack to grow up to the
    #    new soft limit (the kernel extends the main thread's guard page).
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
        if soft != resource.RLIM_INFINITY and stack_size > soft:
            new_soft = stack_size
            if hard != resource.RLIM_INFINITY:
                new_soft = min(stack_size, hard)
            resource.setrlimit(resource.RLIMIT_STACK, (new_soft, hard))
    except (ValueError, OSError):
        pass

    # 2. Set the default stack size for new Python threads (pymongo monitors,
    #    DNS resolver pool, etc.).
    try:
        threading.stack_size(stack_size)
    except (ValueError, OSError):
        pass


def _log(msg: str, *, worker_id: int | None = None) -> None:
    """Write a log line to stderr."""
    if worker_id is not None:
        prefix = f"[worker-{worker_id}]"
    else:
        prefix = "[parent]"
    print(f"{prefix} {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Socket creation
# ---------------------------------------------------------------------------


def _create_listen_socket(
    host: str,
    port: int,
    *,
    reuseport: bool = False,
    backlog: int = 1024,
) -> socket.socket:
    """Create a TCP listening socket.

    Args:
        reuseport: If True, set SO_REUSEPORT so multiple processes
                   can bind to the same address:port.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if reuseport:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((host, port))
        sock.listen(backlog)
    except BaseException:
        sock.close()
        raise
    return sock


# ---------------------------------------------------------------------------
# Connection handling (unchanged from current code)
# ---------------------------------------------------------------------------


def _try_send_error(fd: int, status_code: int) -> None:
    """Best-effort error response. Does NOT close the fd (caller's finally does)."""
    reason = {
        400: "Bad Request",
        413: "Payload Too Large",
        431: "Request Header Fields Too Large",
        500: "Internal Server Error",
        503: "Service Unavailable",
    }
    body = reason.get(status_code, "Error").encode()
    resp = (
        f"HTTP/1.1 {status_code} {reason.get(status_code, 'Error')}\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body
    try:
        _framework_core.green_send(fd, resp)
    except Exception:
        pass  # best effort


# Keep the old _send_error for backward compatibility (used by external code)
def _send_error(fd: int, status_code: int) -> None:
    """Send an HTTP error response and close the connection.

    Best-effort: if the fd is already closed or the send fails, we just
    swallow the exception and move on.

    .. deprecated:: Use _try_send_error instead (does not close fd).
    """
    _try_send_error(fd, status_code)
    try:
        _framework_core.green_close(fd)
    except Exception:
        pass


def _handle_connection(
    fd: int,
    handler: HandlerFunc,
    config: dict[str, int] | None = None,
) -> None:
    """Per-connection greenlet: recv+parse in Zig, dispatch to handler."""
    cfg = config or {}
    max_header_size = cfg.get("max_header_size", 32768)
    max_body_size = cfg.get("max_body_size", 1_048_576)
    try:
        while True:
            request = _framework_core.http_read_request(
                fd,
                max_header_size,
                max_body_size,
            )
            if request is None:
                return  # EOF, finally block closes

            response = Response(fd)
            try:
                handler(request, response)
                response._finalize()
            except Exception:
                # Send 500 if response not yet sent
                if not response._finalized:
                    _try_send_error(fd, 500)
                return
            finally:
                request._invalidate()

            if not request._keep_alive:
                return  # finally block closes

    except ValueError:
        _try_send_error(fd, 400)  # malformed request
    except RuntimeError as e:
        msg = str(e)
        if "header too large" in msg:
            _try_send_error(fd, 431)
        elif "too large" in msg:
            _try_send_error(fd, 413)
        else:
            raise
    except OSError:
        pass  # network error, just close
    finally:
        try:
            _framework_core.green_close(fd)
        except Exception:
            pass


def _run_acceptor(
    listen_fd: int,
    handler: HandlerFunc,
    config: dict[str, int] | None = None,
) -> None:
    """Accept connections and spawn handler greenlets."""
    while True:
        try:
            client_fd = _framework_core.green_accept(listen_fd)
        except OSError as e:
            # Break on errors indicating the socket is closed/invalid:
            # - EBADF: Bad file descriptor (socket closed)
            # - EINVAL: Invalid argument (socket state invalid)
            # - ENOTSOCK: Not a socket
            if e.errno in (errno.EBADF, errno.EINVAL, errno.ENOTSOCK):
                break
            # For other OS errors, log and break (accepting can't continue)
            _log(f"acceptor error: {e}")
            break
        hub_g = _framework_core.get_hub_greenlet()
        g = greenlet.greenlet(
            lambda fd=client_fd: _handle_connection(fd, handler, config),
            parent=hub_g,
        )
        _framework_core.hub_schedule(g)


# ---------------------------------------------------------------------------
# Signal watcher greenlet
# ---------------------------------------------------------------------------


def _signal_watcher(sig_read_fd: int) -> None:
    """Greenlet that monitors the signal wakeup pipe.

    When a signal byte arrives (written by CPython's C-level signal handler
    via set_wakeup_fd), this greenlet resumes and stops the hub.
    """
    _framework_core.green_register_fd(sig_read_fd)
    try:
        data = _framework_core.green_recv(sig_read_fd, 16)
        if data:
            _framework_core.hub_stop()
    except OSError:
        pass
    finally:
        try:
            _framework_core.green_unregister_fd(sig_read_fd)
        except RuntimeError:
            pass  # Hub may already be torn down


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------


def _worker_main(
    worker_id: int,
    handler: HandlerFunc,
    host: str,
    port: int,
    config: dict[str, int] | None = None,
) -> None:
    """Entry point for a forked worker process."""
    _apply_greenlet_stack_config(config)

    # 1. Reset signals - allow graceful shutdown via SIGTERM
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # For SIGTERM, use default handler which will terminate the process
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    # 2. Create socket with SO_REUSEPORT
    try:
        sock = _create_listen_socket(host, port, reuseport=True)
    except OSError as e:
        _log(f"failed to bind {host}:{port}: {e}", worker_id=worker_id)
        os._exit(1)

    listen_fd = sock.fileno()
    _log(f"listening on {host}:{port} (fd={listen_fd})", worker_id=worker_id)

    # 3. Run hub
    from theframework.monkey._state import mark_hub_running, mark_hub_stopped

    try:

        def _acceptor() -> None:
            hub_g = _framework_core.get_hub_greenlet()
            mark_hub_running(hub_g)
            _run_acceptor(listen_fd, handler, config)

        _framework_core.hub_run(listen_fd, _acceptor, config)
    except Exception as e:
        _log(f"hub exited with error: {e}", worker_id=worker_id)
        _log(traceback.format_exc(), worker_id=worker_id)
    finally:
        mark_hub_stopped()
        sock.close()

    os._exit(0)


# ---------------------------------------------------------------------------
# Single-worker mode (current behavior, no fork)
# ---------------------------------------------------------------------------


def _serve_single(
    handler: HandlerFunc,
    host: str,
    port: int,
    *,
    _ready: threading.Event | None = None,
    config: dict[str, int] | None = None,
) -> None:
    """Single-worker mode: no fork, current behavior."""
    _apply_greenlet_stack_config(config)
    sock = _create_listen_socket(host, port, reuseport=False)
    listen_fd = sock.fileno()

    if _ready is not None:
        _ready.set()

    from theframework.monkey._state import mark_hub_running, mark_hub_stopped

    try:

        def _acceptor_with_mark() -> None:
            hub_g = _framework_core.get_hub_greenlet()
            mark_hub_running(hub_g)
            _run_acceptor(listen_fd, handler, config)

        _framework_core.hub_run(listen_fd, _acceptor_with_mark, config)
    finally:
        mark_hub_stopped()
        sock.close()


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------


def _shutdown_children(children: dict[int, int], timeout: float) -> None:
    """Send SIGTERM to all children, wait, then SIGKILL stragglers."""
    if not children:
        return

    _log(f"shutting down {len(children)} workers")

    # Send SIGTERM
    for pid in list(children):
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            children.pop(pid, None)

    # Wait with timeout
    deadline = time.monotonic() + timeout
    while children and time.monotonic() < deadline:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if pid == 0:
            time.sleep(0.01)  # Shorter sleep
            continue
        children.pop(pid, None)
        _log(f"reaped child process {pid}")

    # SIGKILL stragglers
    if children:
        _log(f"killing {len(children)} unresponsive workers")
        for pid in list(children):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        children.clear()

    _log("all workers stopped")


def _supervisor_loop(
    children: dict[int, int],
    handler: HandlerFunc,
    host: str,
    port: int,
    num_workers: int,
    shutdown_timeout: float,
    config: dict[str, int] | None = None,
) -> None:
    """Monitor children, respawn on crash, coordinate shutdown.

    Uses event-driven select() with a self-pipe for signal handling instead of polling.
    Signals (SIGCHLD, SIGTERM, SIGINT) wake up select() for immediate response.
    """
    # Set up self-pipe for signal notifications (self-pipe trick)
    signal_r, signal_w = os.pipe()
    try:
        os.set_blocking(signal_r, False)
        os.set_blocking(signal_w, False)
    except AttributeError:
        # Python 3.9 doesn't have set_blocking, use fcntl
        import fcntl

        fcntl.fcntl(signal_r, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(signal_w, fcntl.F_SETFL, os.O_NONBLOCK)

    # Signal handlers write to the pipe to wake up select()
    def _signal_wakeup(signum: int, frame: object) -> None:
        global _shutting_down
        if signum in (signal.SIGINT, signal.SIGTERM):
            _shutting_down = True
        try:
            os.write(signal_w, b"S")
        except OSError:
            pass

    # Install handlers
    original_sigchld = signal.signal(signal.SIGCHLD, _signal_wakeup)
    original_sigint = signal.signal(signal.SIGINT, _signal_wakeup)
    original_sigterm = signal.signal(signal.SIGTERM, _signal_wakeup)

    crash_history: dict[int, list[float]] = defaultdict(list)

    try:
        while children and not _shutting_down:
            # Wait for signal or timeout (100ms is safe for graceful shutdown response)
            try:
                readable, _, _ = select.select([signal_r], [], [], 0.1)
            except InterruptedError:
                # select() was interrupted by a signal, just continue
                pass
            else:
                # Drain the signal pipe
                if signal_r in readable:
                    try:
                        while True:
                            os.read(signal_r, 4096)
                    except BlockingIOError:
                        pass

            # Reap all exited children (SIGCHLD may have triggered multiple exits)
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break

                if pid <= 0:
                    break  # No more exited children

                # Child exited
                worker_id = children.pop(pid, None)
                if worker_id is None:
                    continue

                if _shutting_down:
                    continue

                # Analyze exit
                if os.WIFEXITED(status):
                    exit_code = os.WEXITSTATUS(status)
                    _log(f"worker {worker_id} (pid {pid}) exited with code {exit_code}")
                elif os.WIFSIGNALED(status):
                    sig = os.WTERMSIG(status)
                    sig_name = signal.Signals(sig).name if hasattr(signal, "Signals") else str(sig)
                    _log(f"worker {worker_id} (pid {pid}) killed by signal {sig_name}")
                else:
                    _log(f"worker {worker_id} (pid {pid}) exited with status {status}")

                # Crash loop detection
                now = time.monotonic()
                history = crash_history[worker_id]
                history[:] = [t for t in history if now - t < CRASH_WINDOW]
                history.append(now)

                if len(history) >= MAX_CRASHES_IN_WINDOW:
                    _log(
                        f"worker {worker_id}: crash loop detected "
                        f"({MAX_CRASHES_IN_WINDOW} crashes in {CRASH_WINDOW}s), "
                        f"not respawning"
                    )
                    continue

                # Exponential backoff before respawn
                crash_count = len(history)
                if crash_count > 1:
                    # Exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s …, capped at 5s
                    backoff = min(0.1 * (2 ** (crash_count - 2)), 5.0)
                    _log(
                        f"worker {worker_id}: crash #{crash_count}, "
                        f"backing off {backoff:.1f}s before respawn",
                        worker_id=None,
                    )
                    time.sleep(backoff)

                # Respawn
                new_pid = os.fork()
                if new_pid == 0:
                    # Child process: _worker_main will set up signal handlers
                    _worker_main(worker_id, handler, host, port, config=config)
                    os._exit(0)
                children[new_pid] = worker_id
                _log(f"worker {worker_id}: respawned as pid {new_pid}")

        # Check why we exited the loop
        if not children and not _shutting_down:
            # All workers removed but no shutdown signal received
            # This means all workers hit crash loop detection
            _log("FATAL: all workers stopped due to crash loops")

        _shutdown_children(children, shutdown_timeout)
    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGCHLD, original_sigchld)
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        os.close(signal_r)
        os.close(signal_w)


# ---------------------------------------------------------------------------
# Multi-worker mode
# ---------------------------------------------------------------------------


def _serve_prefork(
    handler: HandlerFunc,
    host: str,
    port: int,
    *,
    workers: int,
    config: dict[str, int] | None = None,
) -> None:
    """Multi-worker mode: fork N children, each with SO_REUSEPORT."""
    global _shutting_down
    _shutting_down = False

    _log(f"starting {workers} workers on {host}:{port}")

    children: dict[int, int] = {}  # pid → worker_id

    try:
        # Fork workers
        for worker_id in range(workers):
            pid = os.fork()
            if pid == 0:
                # Child process: _worker_main will set up signal handlers
                _worker_main(worker_id, handler, host, port, config=config)
                os._exit(0)  # Should not reach here
            children[pid] = worker_id
            _log(f"worker {worker_id}: forked as pid {pid}")

        # Enter supervisor loop
        _supervisor_loop(
            children,
            handler,
            host,
            port,
            workers,
            SHUTDOWN_TIMEOUT,
            config=config,
        )
    except KeyboardInterrupt:
        _log("interrupted by user")
        _shutdown_children(children, SHUTDOWN_TIMEOUT)
    finally:
        _shutting_down = False
        _log("parent exiting")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def serve(
    handler: HandlerFunc,
    host: str = "127.0.0.1",
    port: int = 8000,
    *,
    workers: int = 1,
    _ready: threading.Event | None = None,
    config: dict[str, int] | None = None,
) -> None:
    """Start the HTTP server.

    Args:
        handler: Function called for each HTTP request.
        host: Address to bind to.
        port: Port to bind to.
        workers: Number of worker processes. Defaults to 1 (single-process,
                 no fork). Set to 0 to auto-detect from CPU count.
        _ready: Event set when the server is ready (single-worker only).
        config: Optional configuration dict passed to the Zig hub. Supported
                keys: max_header_size, max_body_size, max_connections,
                chunk_size, read_timeout_ms, keepalive_timeout_ms,
                handler_timeout_ms, greenlet_stack_size (bytes, e.g.
                8388608 for 8 MiB — increases the OS stack limit so
                greenlets can handle deep call chains).
    """
    if workers == 0:
        workers = os.cpu_count() or 1

    if workers == 1:
        _serve_single(handler, host, port, _ready=_ready, config=config)
    else:
        if _ready is not None:
            raise ValueError("_ready is not supported with workers > 1")
        _serve_prefork(handler, host, port, workers=workers, config=config)

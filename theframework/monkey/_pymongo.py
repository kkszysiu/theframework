"""Monkey-patch ``pymongo`` for cooperative MongoDB I/O.

pymongo's internal network I/O interacts with our cooperative socket layer
in ways that can cause deep C-stack recursion and segfaults on CPython 3.14
with greenlets.

Strategy: replace pymongo's key I/O entry points with versions that run
the blocking call in a background thread and cooperatively wait for the
result via a pipe — the same pattern used for DNS resolution in
``_dns.py``.  pymongo sockets are created and used entirely in worker
threads (where ``hub_is_running()`` is False), so they stay in plain
blocking mode and never touch io_uring.  The calling greenlet yields to
the hub and resumes when the thread finishes.

The three patching points:

1. ``pymongo.pool._configured_socket`` — socket creation + connect + SSL.
   Running this in a thread ensures the socket is never registered with
   the hub and stays blocking.

2. ``pymongo.network.command`` — sends a command and reads the response.
   This is the main I/O entry point for all MongoDB operations.

3. ``pymongo.network.receive_message`` — reads a response (used for
   cursor iteration via ``getMore``).

Inside the worker thread, ``hub_is_running()`` returns ``False``, so all
our monkey-patched socket/select/ssl functions fall through to their
original blocking implementations.  No nested thread spawning occurs
because ``_run_in_thread`` checks ``hub_is_running()`` first.
"""

from __future__ import annotations

import os as _os
from concurrent.futures import Future, ThreadPoolExecutor

import _framework_core

from theframework.monkey._state import (
    get_original as _get_original,
    hub_is_running as _hub_is_running,
)

# Poll event constant
_POLLIN: int = 0x001

# ---------------------------------------------------------------------------
# Thread pool (lazy singleton, separate from DNS pool)
# ---------------------------------------------------------------------------

_mongo_pool: ThreadPoolExecutor | None = None


def _get_pool() -> ThreadPoolExecutor:
    global _mongo_pool
    if _mongo_pool is None:
        _mongo_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="mongo-io")
    return _mongo_pool


# ---------------------------------------------------------------------------
# Core: run a blocking callable in the mongo thread pool, wake via pipe
# ---------------------------------------------------------------------------


def _run_in_thread(fn: object, *args: object, **kwargs: object) -> object:
    """Run *fn(*args, **kwargs)* in the mongo I/O thread pool.

    If the hub is not running (e.g. we are already in a worker thread),
    call *fn* directly — this prevents nested thread spawning when a
    wrapped function calls another wrapped function.
    """
    if not _hub_is_running():
        return fn(*args, **kwargs)  # type: ignore[operator]

    pool = _get_pool()

    r_fd, w_fd = _os.pipe()
    _os.set_blocking(r_fd, False)

    result_box: list[object] = [None]
    error_box: list[BaseException | None] = [None]

    def _worker() -> None:
        try:
            result_box[0] = fn(*args, **kwargs)  # type: ignore[operator]
        except BaseException as exc:
            error_box[0] = exc
        finally:
            try:
                _os.write(w_fd, b"\x00")
            except OSError:
                pass

    future: Future[None] = pool.submit(_worker)  # noqa: F841

    try:
        _framework_core.green_poll_fd(r_fd, _POLLIN)
        try:
            _os.read(r_fd, 1)
        except OSError:
            pass
    finally:
        _os.close(r_fd)
        _os.close(w_fd)

    if error_box[0] is not None:
        raise error_box[0]
    return result_box[0]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_patched = False


def patch_pymongo() -> None:
    """Make pymongo use thread-based cooperative I/O.

    Call this AFTER ``patch_all()`` and AFTER importing pymongo (or any
    library that imports pymongo).
    """
    global _patched
    if _patched:
        return

    try:
        import pymongo.network as _network
        import pymongo.pool as _pool
    except ImportError:
        return  # pymongo not installed, nothing to patch

    # --- 1. Socket creation + connect + SSL ---
    _orig_configured_socket = _pool._configured_socket

    def _green_configured_socket(*args: object, **kwargs: object) -> object:
        return _run_in_thread(_orig_configured_socket, *args, **kwargs)

    _pool._configured_socket = _green_configured_socket  # type: ignore[assignment]

    # --- 2. command (send + receive) ---
    _orig_command = _network.command

    def _green_command(*args: object, **kwargs: object) -> object:
        return _run_in_thread(_orig_command, *args, **kwargs)

    _network.command = _green_command  # type: ignore[assignment]

    # --- 3. receive_message (cursor reads) ---
    _orig_receive_message = _network.receive_message

    def _green_receive_message(*args: object, **kwargs: object) -> object:
        return _run_in_thread(_orig_receive_message, *args, **kwargs)

    _network.receive_message = _green_receive_message  # type: ignore[assignment]

    # --- 4. SocketChecker: use original select.poll ---
    try:
        import pymongo.socket_checker as _sc

        _orig_poll_cls = _get_original("select", "poll")

        class _OriginalSocketChecker(_sc.SocketChecker):
            def __init__(self) -> None:
                if _sc._HAVE_POLL and _orig_poll_cls is not None:
                    self._poller = _orig_poll_cls()  # type: ignore[operator]
                else:
                    self._poller = None

        _sc.SocketChecker = _OriginalSocketChecker  # type: ignore[misc]
    except (ImportError, AttributeError):
        pass

    _patched = True

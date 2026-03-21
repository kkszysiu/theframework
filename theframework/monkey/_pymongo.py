"""Monkey-patch ``pymongo`` for cooperative MongoDB I/O.

PyMongo's synchronous networking code uses background monitor threads, its own
socket polling helpers, and several imported function aliases. Those paths can
interact badly with our cooperative stdlib monkey patches when they execute in
arbitrary threads.

Strategy: funnel all sync PyMongo network I/O through a dedicated worker pool.
Hub threads wait cooperatively via pipe + green_poll_fd. Plain background
threads block on ``Future.result()``. Once execution is inside a mongo worker,
nested PyMongo calls run inline so sockets stay on one normal blocking thread
and never bounce through the hub's monkey-patched polling stack.

The patch supports both the older module layout (``pymongo.pool`` /
``pymongo.network``) and the current one (``pymongo.synchronous.pool`` /
``pymongo.synchronous.network`` / ``pymongo.network_layer``).
"""

from __future__ import annotations

import os as _os
import threading as _threading
from concurrent.futures import Future, ThreadPoolExecutor
from importlib import import_module as _import_module

import _framework_core

from theframework.monkey._state import hub_is_running as _hub_is_running

# Poll event constant
_POLLIN: int = 0x001

# ---------------------------------------------------------------------------
# Thread pool (lazy singleton, separate from DNS pool)
# ---------------------------------------------------------------------------

_mongo_pool: ThreadPoolExecutor | None = None
_worker_local = _threading.local()


def _get_pool() -> ThreadPoolExecutor:
    global _mongo_pool
    if _mongo_pool is None:
        raw_size = _os.getenv("THEFRAMEWORK_MONGO_IO_WORKERS")
        if raw_size is not None:
            try:
                max_workers = max(1, int(raw_size))
            except ValueError:
                max_workers = 32
        else:
            max_workers = 32
        _mongo_pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mongo-io")
    return _mongo_pool


def _in_mongo_worker() -> bool:
    return bool(getattr(_worker_local, "active", False))


# ---------------------------------------------------------------------------
# Core: run a blocking callable in the mongo thread pool, wake via pipe
# ---------------------------------------------------------------------------


def _run_in_thread(fn: object, *args: object, **kwargs: object) -> object:
    """Run *fn(*args, **kwargs)* in the mongo I/O thread pool.

    Hub threads wait cooperatively for the worker via pipe + green_poll_fd.
    Non-hub threads block on ``Future.result()``. Calls from inside a mongo
    worker run inline so nested PyMongo helpers stay on the same OS thread.
    """
    if _in_mongo_worker():
        return fn(*args, **kwargs)  # type: ignore[operator]

    pool = _get_pool()

    def _call() -> object:
        _worker_local.active = True
        try:
            return fn(*args, **kwargs)  # type: ignore[operator]
        finally:
            _worker_local.active = False

    if not _hub_is_running():
        return pool.submit(_call).result()

    r_fd, w_fd = _os.pipe()
    _os.set_blocking(r_fd, False)

    result_box: list[object] = [None]
    error_box: list[BaseException | None] = [None]

    def _worker() -> None:
        try:
            result_box[0] = _call()
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


def _maybe_import(name: str) -> object | None:
    try:
        return _import_module(name)
    except ImportError:
        return None


def _wrap_via_thread(wrapped: dict[int, object], fn: object) -> object:
    key = id(fn)
    if key not in wrapped:
        def _wrapper(*args: object, **kwargs: object) -> object:
            return _run_in_thread(fn, *args, **kwargs)

        wrapped[key] = _wrapper
    return wrapped[key]


def _patch_attr(module: object, attr: str, wrapped: dict[int, object]) -> None:
    if not hasattr(module, attr):
        return
    original = getattr(module, attr)
    setattr(module, attr, _wrap_via_thread(wrapped, original))


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

    pool_modules = [
        module
        for module in (
            _maybe_import("pymongo.pool"),
            _maybe_import("pymongo.synchronous.pool"),
        )
        if module is not None
    ]
    network_modules = [
        module
        for module in (
            _maybe_import("pymongo.network"),
            _maybe_import("pymongo.synchronous.network"),
            _maybe_import("pymongo.network_layer"),
        )
        if module is not None
    ]
    if not pool_modules and not network_modules:
        return

    configured_wrapped: dict[int, object] = {}
    for module in pool_modules:
        _patch_attr(module, "_configured_socket", configured_wrapped)
        _patch_attr(module, "_configured_socket_interface", configured_wrapped)

    command_wrapped: dict[int, object] = {}
    for module in (*network_modules, *pool_modules):
        _patch_attr(module, "command", command_wrapped)

    receive_wrapped: dict[int, object] = {}
    for module in (*network_modules, *pool_modules):
        _patch_attr(module, "receive_message", receive_wrapped)

    # --- Connection.send_message (query/insert sends) ---
    # server.run_operation() calls conn.send_message() then conn.receive_message()
    # as separate calls.  Without wrapping send_message, self.conn.sendall() runs
    # in the greenlet context which registers the socket with io_uring (sets it
    # non-blocking).  The subsequent receive_message (wrapped) then fails in its
    # worker thread because recv_into hits BlockingIOError on the non-blocking fd.
    send_wrapped: dict[int, object] = {}
    for module in pool_modules:
        conn_cls = getattr(module, "Connection", None)
        if conn_cls is not None:
            _patch_attr(conn_cls, "send_message", send_wrapped)

    _patched = True

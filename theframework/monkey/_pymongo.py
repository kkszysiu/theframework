"""Monkey-patch ``pymongo`` for cooperative MongoDB I/O.

PyMongo's synchronous networking code uses background monitor threads, its own
socket polling helpers, and several imported function aliases. Those paths can
interact badly with our cooperative stdlib monkey patches when they execute in
arbitrary threads.

Strategy: funnel MongoDB socket creation and blocking network I/O through a
dedicated worker pool. The hub thread waits cooperatively for completion; plain
background threads simply block on the worker future. Either way, the actual
Mongo I/O runs in ``mongo-io_*`` threads where ``hub_is_running()`` is false,
so sockets stay in plain blocking mode and never touch io_uring.

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
_worker_local = _threading.local()


def _get_pool() -> ThreadPoolExecutor:
    global _mongo_pool
    if _mongo_pool is None:
        _mongo_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="mongo-io")
    return _mongo_pool


def _in_mongo_worker() -> bool:
    return bool(getattr(_worker_local, "active", False))


# ---------------------------------------------------------------------------
# Core: run a blocking callable in the mongo thread pool, wake via pipe
# ---------------------------------------------------------------------------


def _run_in_thread(fn: object, *args: object, **kwargs: object) -> object:
    """Run *fn(*args, **kwargs)* in the mongo I/O thread pool.

    Hub threads wait cooperatively for the worker. Non-hub threads block on the
    Future result. Calls originating from inside a mongo worker run inline to
    avoid nested thread spawning.
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
        future: Future[object] = pool.submit(_call)
        return future.result()

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


def _patch_socket_checker(module: object) -> None:
    if not hasattr(module, "SocketChecker"):
        return

    socket_checker_cls = module.SocketChecker
    orig_poll_cls = _get_original("select", "poll")
    if orig_poll_cls is None:
        return
    try:
        orig_poller_type = type(orig_poll_cls())  # type: ignore[operator]
    except TypeError:
        orig_poller_type = None

    def _ensure_original_poller(self: object) -> None:
        if not getattr(module, "_HAVE_POLL", False):
            return
        poller = getattr(self, "_poller", None)
        if poller is None:
            self._poller = orig_poll_cls()  # type: ignore[operator]
            return
        if orig_poller_type is not None and not isinstance(poller, orig_poller_type):
            self._poller = orig_poll_cls()  # type: ignore[operator]

    orig_init = socket_checker_cls.__init__

    def _patched_init(self: object, *args: object, **kwargs: object) -> None:
        orig_init(self, *args, **kwargs)
        _ensure_original_poller(self)

    orig_select = socket_checker_cls.select

    def _patched_select(self: object, *args: object, **kwargs: object) -> object:
        _ensure_original_poller(self)
        return orig_select(self, *args, **kwargs)

    socket_checker_cls.__init__ = _patched_init
    socket_checker_cls.select = _patched_select


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
    socket_checker = _maybe_import("pymongo.socket_checker")

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

    if socket_checker is not None:
        _patch_socket_checker(socket_checker)

    _patched = True

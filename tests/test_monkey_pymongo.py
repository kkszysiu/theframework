from __future__ import annotations

import socket
import threading

import pytest

from theframework.monkey import patch_all, patch_pymongo
from theframework.monkey._pymongo import _run_in_thread


def test_run_in_thread_uses_mongo_worker_from_plain_thread() -> None:
    results: list[str] = []
    done = threading.Event()

    def _caller() -> None:
        worker_name = _run_in_thread(lambda: threading.current_thread().name)
        results.append(worker_name)
        done.set()

    thread = threading.Thread(name="plain-caller", target=_caller)
    thread.start()
    done.wait(timeout=5)
    thread.join(timeout=5)

    assert results
    assert results[0].startswith("mongo-io")


def test_run_in_thread_does_not_nest_inside_mongo_worker() -> None:
    def _outer() -> tuple[str, str]:
        outer_name = threading.current_thread().name
        inner_name = _run_in_thread(lambda: threading.current_thread().name)
        return outer_name, inner_name

    outer_name, inner_name = _run_in_thread(_outer)

    assert outer_name.startswith("mongo-io")
    assert inner_name == outer_name


def test_patch_pymongo_updates_current_aliases_and_socket_checker() -> None:
    pymongo = pytest.importorskip("pymongo")

    patch_all()

    import pymongo.socket_checker as socket_checker

    sync_pool = pytest.importorskip("pymongo.synchronous.pool")
    orig_pool_command = sync_pool.command
    orig_pool_receive = sync_pool.receive_message
    orig_select = socket_checker.SocketChecker.select

    patch_pymongo()

    assert sync_pool.command is not orig_pool_command
    assert sync_pool.receive_message is not orig_pool_receive
    assert socket_checker.SocketChecker.select is not orig_select

    checker = socket_checker.SocketChecker()
    assert checker._poller is None

    left, right = socket.socketpair()
    try:
        assert checker.select(left, write=True, timeout=0.0) is True
    finally:
        left.close()
        right.close()

    import pymongo.pool as pool

    assert pool.command is not orig_pool_command
    if hasattr(pool, "receive_message"):
        assert pool.receive_message is not orig_pool_receive

    assert pymongo.version

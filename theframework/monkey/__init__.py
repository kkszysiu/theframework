"""Monkey-patching for stdlib modules.

Call :func:`patch_all` **before** importing any third-party library that
uses blocking I/O.  Every patched module delegates its I/O operations to
the io_uring hub so that greenlets yield cooperatively instead of
blocking the OS thread.

Example::

    from theframework.monkey import patch_all
    patch_all()
    # Now import requests, urllib3, etc. — they will be cooperative.
"""

from __future__ import annotations

from theframework.monkey._state import _saved, is_module_patched, patch_item, remove_item

__all__ = [
    "patch_all",
    "patch_socket",
    "patch_time",
    "patch_select",
    "patch_selectors",
    "patch_ssl",
    "patch_pymongo",
    "is_patched",
]


def is_patched(mod_name: str) -> bool:
    """Return ``True`` if *mod_name* has been monkey-patched."""
    return is_module_patched(mod_name)


# ---------------------------------------------------------------------------
# Per-module patchers
# ---------------------------------------------------------------------------


def patch_time() -> None:
    """Replace ``time.sleep`` with a cooperative version."""
    if is_module_patched("time"):
        return

    import time

    from theframework.monkey._time import sleep

    patch_item(time, "sleep", sleep)


def patch_socket() -> None:
    """Replace ``socket.socket`` and DNS functions."""
    if is_module_patched("socket"):
        return

    import socket

    from theframework.monkey._dns import (
        getaddrinfo,
        gethostbyaddr,
        gethostbyname,
        gethostbyname_ex,
        getnameinfo,
    )
    from theframework.monkey._socket import (
        create_connection,
        fromfd,
        socket as CoopSocket,
        socketpair,
    )

    patch_item(socket, "socket", CoopSocket)
    patch_item(socket, "SocketType", CoopSocket)
    patch_item(socket, "create_connection", create_connection)
    patch_item(socket, "socketpair", socketpair)
    patch_item(socket, "fromfd", fromfd)

    # DNS — run blocking lookups in a thread pool, wake via pipe
    patch_item(socket, "getaddrinfo", getaddrinfo)
    patch_item(socket, "gethostbyname", gethostbyname)
    patch_item(socket, "gethostbyname_ex", gethostbyname_ex)
    patch_item(socket, "gethostbyaddr", gethostbyaddr)
    patch_item(socket, "getnameinfo", getnameinfo)


def patch_select() -> None:
    """Replace ``select.select`` and ``select.poll``."""
    if is_module_patched("select"):
        return

    import select

    from theframework.monkey._select import poll as CoopPoll
    from theframework.monkey._select import select as coop_select

    patch_item(select, "select", coop_select)
    if hasattr(select, "poll"):
        patch_item(select, "poll", CoopPoll)

    # Remove epoll/kqueue to force code through our patched select/poll.
    for attr in ("epoll", "kqueue", "kevent", "devpoll"):
        if hasattr(select, attr):
            remove_item(select, attr)


def patch_selectors() -> None:
    """Replace ``selectors.DefaultSelector``."""
    if is_module_patched("selectors"):
        return

    import selectors

    from theframework.monkey._selectors import CooperativeSelector

    patch_item(selectors, "DefaultSelector", CooperativeSelector)

    # Remove high-performance selectors to force DefaultSelector usage.
    for attr in ("EpollSelector", "KqueueSelector", "DevpollSelector"):
        if hasattr(selectors, attr):
            remove_item(selectors, attr)


def patch_ssl() -> None:
    """Configure ``SSLContext`` to create cooperative SSL sockets.

    We do NOT replace ``ssl.SSLSocket`` at module level because its
    ``_create`` classmethod contains ``super(SSLSocket, self).__init__()``
    where ``SSLSocket`` is resolved from module globals at call time.
    Replacing the name changes the super-resolution chain and causes
    ``original_SSLSocket.__init__`` (which raises ``TypeError``) to be
    called instead of ``socket.socket.__init__``.

    We also do NOT replace ``ssl.SSLContext`` because the stdlib's
    property setters use ``super(SSLContext, SSLContext)`` which would
    cause infinite recursion when the name is rebound.
    """
    if is_module_patched("ssl"):
        return

    import ssl

    from theframework.monkey._ssl import SSLSocket

    # Record original for is_module_patched("ssl") tracking.
    _saved.setdefault("ssl", {}).setdefault("SSLSocket", ssl.SSLSocket)
    # Only set sslsocket_class — wrap_socket() uses this to create sockets.
    # isinstance(x, ssl.SSLSocket) still works because our class is a subclass.
    ssl.SSLContext.sslsocket_class = SSLSocket


def patch_pymongo() -> None:
    """Make pymongo use thread-based cooperative I/O.

    Call this AFTER ``patch_all()`` and AFTER importing pymongo.
    Replaces pymongo's socket creation and low-level network I/O so
    blocking MongoDB operations run in a background thread pool while
    the calling greenlet yields cooperatively.
    """
    from theframework.monkey._pymongo import patch_pymongo as _do_patch

    _do_patch()


# ---------------------------------------------------------------------------
# patch_all
# ---------------------------------------------------------------------------


def patch_all(
    *,
    socket: bool = True,
    time: bool = True,
    select: bool = True,
    selectors: bool = True,
    ssl: bool = True,
) -> None:
    """Apply all monkey-patches.

    Call this **once**, at import time, before any third-party code
    has a chance to cache references to the original modules.

    Parameters default to ``True``; pass ``False`` to skip a module.
    """
    if time:
        patch_time()
    if socket:
        patch_socket()
    if select:
        patch_select()
    if selectors:
        patch_selectors()
    if ssl:
        patch_ssl()

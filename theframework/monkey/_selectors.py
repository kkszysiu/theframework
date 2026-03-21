"""Monkey-patch ``selectors.DefaultSelector`` for cooperative I/O."""
# mypy: disable-error-code="explicit-any"
# SelectorKey is a stdlib NamedTuple with ``data: Any`` that we cannot
# change.  Every annotation referencing SelectorKey triggers explicit-any.

from __future__ import annotations

import selectors as _orig_selectors_mod
import time as _time_mod

import _framework_core

from theframework.monkey._state import (
    get_fd as _get_fd,
    get_original,
    hub_is_running as _hub_is_running,
)


def _get_original_select(
    rlist: list[object],
    wlist: list[object],
    xlist: list[object],
    timeout: float,
) -> tuple[list[object], list[object], list[object]]:
    """Call the real (unpatched) select.select."""
    fn = get_original("select", "select")
    result: tuple[list[object], list[object], list[object]] = fn(rlist, wlist, xlist, timeout)  # type: ignore[operator]
    return result


_SKey = _orig_selectors_mod.SelectorKey

_POLLIN = 0x001
_POLLOUT = 0x004


class CooperativeSelector(_orig_selectors_mod.BaseSelector):
    """A cooperative selector backed by io_uring.

    Subclasses ``selectors.BaseSelector`` so that
    ``isinstance(sel, selectors.BaseSelector)`` returns ``True``.
    """

    def __init__(self) -> None:
        super().__init__()
        self._keys: dict[int, _SKey] = {}
        self._closed: bool = False

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("selector is closed")

    def register(
        self,
        fileobj: object,
        events: int,
        data: object = None,
    ) -> _SKey:
        self._check_closed()
        fd = _get_fd(fileobj)
        if fd in self._keys:
            raise KeyError(f"fd {fd} already registered")
        key = _SKey(
            fileobj=fileobj,  # type: ignore[arg-type]
            fd=fd,
            events=events,
            data=data,
        )
        self._keys[fd] = key
        return key

    def unregister(self, fileobj: object) -> _SKey:
        self._check_closed()
        fd = _get_fd(fileobj)
        return self._keys.pop(fd)

    def select(self, timeout: float | None = None) -> list[tuple[_SKey, int]]:
        self._check_closed()
        if not self._keys:
            if timeout is not None and timeout > 0:
                if _hub_is_running():
                    _framework_core.green_sleep(timeout)
                else:
                    _time_mod.sleep(timeout)
            return []

        # Non-blocking check first
        if timeout is not None and timeout == 0:
            rlist, wlist = self._build_fd_lists()
            r, w, _ = _get_original_select(rlist, wlist, [], 0)
            return self._build_results(r, w)

        # When no hub is running, fall back to original select
        if not _hub_is_running():
            rlist, wlist = self._build_fd_lists()
            try:
                r, w, _ = _get_original_select(rlist, wlist, [], timeout)
            except (ValueError, OSError):
                return []
            return self._build_results(r, w)

        # Hub is running: use green_poll_multi (event-driven, no busy-wait)
        fds_list: list[int] = []
        events_list: list[int] = []
        for fd, key in self._keys.items():
            poll_events = 0
            if key.events & _orig_selectors_mod.EVENT_READ:
                poll_events |= _POLLIN
            if key.events & _orig_selectors_mod.EVENT_WRITE:
                poll_events |= _POLLOUT
            fds_list.append(fd)
            events_list.append(poll_events)

        timeout_ms = -1
        if timeout is not None:
            timeout_ms = max(1, int(timeout * 1000))

        ready = _framework_core.green_poll_multi(fds_list, events_list, timeout_ms)

        # Map ready results to SelectorKey
        result_map: dict[int, tuple[_SKey, int]] = {}
        for fd, revents in ready:
            if fd not in self._keys:
                continue
            sel_events = 0
            if revents & (_POLLIN | 0x008 | 0x010):  # POLLIN | POLLERR | POLLHUP
                sel_events |= _orig_selectors_mod.EVENT_READ
            if revents & (_POLLOUT | 0x008 | 0x010):  # POLLOUT | POLLERR | POLLHUP
                sel_events |= _orig_selectors_mod.EVENT_WRITE
            # Mask to only report events that were registered
            sel_events &= self._keys[fd].events
            if sel_events:
                result_map[fd] = (self._keys[fd], sel_events)

        return list(result_map.values())

    def _build_fd_lists(self) -> tuple[list[object], list[object]]:
        rlist: list[object] = [
            k.fileobj for k in self._keys.values() if k.events & _orig_selectors_mod.EVENT_READ
        ]
        wlist: list[object] = [
            k.fileobj for k in self._keys.values() if k.events & _orig_selectors_mod.EVENT_WRITE
        ]
        return rlist, wlist

    def _build_results(self, rlist: list[object], wlist: list[object]) -> list[tuple[_SKey, int]]:
        result_map: dict[int, tuple[_SKey, int]] = {}
        for obj in rlist:
            fd = _get_fd(obj)
            if fd in self._keys:
                result_map[fd] = (self._keys[fd], _orig_selectors_mod.EVENT_READ)
        for obj in wlist:
            fd = _get_fd(obj)
            if fd in self._keys:
                if fd in result_map:
                    key, events = result_map[fd]
                    result_map[fd] = (key, events | _orig_selectors_mod.EVENT_WRITE)
                else:
                    result_map[fd] = (self._keys[fd], _orig_selectors_mod.EVENT_WRITE)
        return list(result_map.values())

    def get_key(self, fileobj: object) -> _SKey:
        self._check_closed()
        fd = _get_fd(fileobj)
        return self._keys[fd]

    def get_map(self) -> dict[int, _SKey]:  # type: ignore[override]
        self._check_closed()
        return dict(self._keys)

    def close(self) -> None:
        self._keys.clear()
        self._closed = True

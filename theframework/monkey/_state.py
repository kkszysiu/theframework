"""Core monkey-patching infrastructure: save, patch, restore."""

from __future__ import annotations

import _thread
import sys
from types import ModuleType

import greenlet

# Maps "module_name" -> {"attr_name": original_value}
_saved: dict[str, dict[str, object]] = {}


def _save(module: ModuleType, attr: str, value: object) -> None:
    """Save an original value (first save wins)."""
    mod_name = module.__name__
    _saved.setdefault(mod_name, {}).setdefault(attr, value)


def patch_item(module: ModuleType, attr: str, new_value: object) -> None:
    """Replace *module.attr* with *new_value*, saving the original."""
    old = getattr(module, attr, _MISSING)
    if old is not _MISSING:
        _save(module, attr, old)
    setattr(module, attr, new_value)


def remove_item(module: ModuleType, attr: str) -> None:
    """Remove *module.attr*, saving the original for later restoration."""
    old = getattr(module, attr, _MISSING)
    if old is not _MISSING:
        _save(module, attr, old)
        delattr(module, attr)


def get_original(mod_name: str, attr: str) -> object:
    """Return the original value of *mod_name.attr* before patching."""
    if mod_name in _saved and attr in _saved[mod_name]:
        return _saved[mod_name][attr]
    return getattr(sys.modules[mod_name], attr)


def is_module_patched(mod_name: str) -> bool:
    return mod_name in _saved


# ---------------------------------------------------------------------------
# Hub detection — O(1) via thread-local storage
# ---------------------------------------------------------------------------

# Each thread gets its own ``_hub_greenlet`` attribute.  The hub thread sets
# it to the hub greenlet object when ``hub_run`` starts, and clears it when
# ``hub_run`` returns.  Non-hub threads never see a non-None value.
#
# We use ``_thread._local`` directly (not ``threading.local``) because it is
# the C-level implementation and avoids importing the heavyweight
# ``threading`` module at patch time.


class _HubLocal(_thread._local):
    hub_greenlet: greenlet.greenlet | None

    def __init__(self) -> None:
        super().__init__()
        self.hub_greenlet = None


_hub_local = _HubLocal()


def mark_hub_running(hub_g: greenlet.greenlet) -> None:
    """Mark the current thread as running the hub.

    Called from the Python wrapper around ``_framework_core.hub_run``.
    """
    _hub_local.hub_greenlet = hub_g


def mark_hub_stopped() -> None:
    """Mark the current thread as no longer running the hub."""
    _hub_local.hub_greenlet = None


def hub_is_running() -> bool:
    """Return True if the io_uring hub is active in the current thread.

    This is intentionally a pure thread-local check. Background threads
    that hit monkey-patched stdlib functions must never touch the process
    global hub state just to decide whether they should take the
    cooperative path.
    """
    return _hub_local.hub_greenlet is not None


def get_fd(obj: object) -> int:
    """Extract a file descriptor from a socket, fd int, or anything
    with a ``fileno()`` method."""
    if isinstance(obj, int):
        return obj
    fileno_meth = getattr(obj, "fileno", None)
    if fileno_meth is not None:
        return int(fileno_meth())
    raise TypeError(f"expected int or object with fileno(), got {type(obj)}")


class _Missing:
    """Sentinel for missing attributes."""


_MISSING = _Missing()

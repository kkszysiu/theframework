"""Custom Hatch build hook that runs ``zig build`` and bundles the resulting
shared libraries into the wheel.

Artifacts produced by ``zig build``:

* ``zig-out/lib/libframework.{so,dylib}``  — loaded via ctypes
* ``zig-out/lib/_framework_core<EXT_SUFFIX>.so`` — Python C extension

The hook copies them so they end up in the installed package:

* ``theframework/libframework.{so,dylib}`` — next to ``__init__.py``
* ``_framework_core<EXT_SUFFIX>.so`` — top-level in site-packages (bare import)
"""

from __future__ import annotations

import glob
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class ZigBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _lib_name() -> str:
        """Return the platform-specific name of the shared library."""
        if platform.system() == "Darwin":
            return "libframework.dylib"
        return "libframework.so"

    @staticmethod
    def _ext_glob() -> str:
        """Glob pattern that matches the Python C extension."""
        return "_framework_core*.so"

    @staticmethod
    def _detect_zig_options() -> list[str]:
        """Detect Python/greenlet paths from the current interpreter and
        return them as ``-D`` flags for ``zig build``."""
        import greenlet

        flags = []

        python_include = sysconfig.get_path("include")
        if python_include:
            flags.append(f"-Dpython-include={python_include}")

        greenlet_dir = os.path.dirname(greenlet.__file__)
        flags.append(f"-Dgreenlet-include={greenlet_dir}")

        python_lib = sysconfig.get_config_var("LIBDIR")
        if python_lib:
            flags.append(f"-Dpython-lib={python_lib}")

        ext_suffix = sysconfig.get_config_var("EXT_SUFFIX")
        if ext_suffix:
            # build.zig expects the suffix *without* the trailing .so
            flags.append(f"-Dext-suffix={ext_suffix.removesuffix('.so')}")

        return flags

    # ------------------------------------------------------------------
    # hook entry point
    # ------------------------------------------------------------------

    def initialize(self, version: str, build_data: dict) -> None:  # type: ignore[type-arg]
        root = Path(self.root)
        zig_lib = root / "zig-out" / "lib"

        # 1. Run ``zig build`` with explicit Python paths ----------------
        cmd = ["zig", "build", *self._detect_zig_options()]
        subprocess.check_call(cmd, cwd=str(root))

        # 2. Locate artifacts --------------------------------------------
        lib_src = zig_lib / self._lib_name()
        if not lib_src.exists():
            msg = f"zig build did not produce {lib_src}"
            raise RuntimeError(msg)

        ext_matches = glob.glob(str(zig_lib / self._ext_glob()))
        if not ext_matches:
            msg = f"zig build did not produce {self._ext_glob()} in {zig_lib}"
            raise RuntimeError(msg)
        ext_src = Path(ext_matches[0])

        # 3. Copy into package directory ----------------------------------
        pkg_dir = root / "theframework"
        lib_dst = pkg_dir / lib_src.name
        shutil.copy2(str(lib_src), str(lib_dst))

        # _framework_core goes to wheel root (site-packages top-level)
        # so bare ``import _framework_core`` keeps working.
        ext_dst = root / ext_src.name
        shutil.copy2(str(ext_src), str(ext_dst))

        # 4. Tell hatchling to include them in the wheel ------------------
        build_data["force_include"][str(lib_dst)] = f"theframework/{lib_src.name}"
        build_data["force_include"][str(ext_dst)] = ext_src.name

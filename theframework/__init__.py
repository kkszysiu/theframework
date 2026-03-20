import ctypes
import pathlib
import platform
import sys

_PACKAGE_DIR = pathlib.Path(__file__).resolve().parent
_LIB_NAME = "libframework.dylib" if platform.system() == "Darwin" else "libframework.so"
_LIB_PATH = _PACKAGE_DIR / _LIB_NAME

if not _LIB_PATH.exists():
    # Fallback for development: load from zig-out/lib/
    _PROJECT_ROOT = _PACKAGE_DIR.parent
    _LIB_PATH = _PROJECT_ROOT / "zig-out" / "lib" / _LIB_NAME
    # Also add zig-out/lib to sys.path so `import _framework_core` works
    _ext_dir = str(_PROJECT_ROOT / "zig-out" / "lib")
    if _ext_dir not in sys.path:
        sys.path.insert(0, _ext_dir)

_lib = ctypes.CDLL(str(_LIB_PATH))
_lib.framework_version.restype = ctypes.c_int32
_lib.framework_version.argtypes = []


def version() -> int:
    result: int = _lib.framework_version()
    return result


from theframework.app import Framework as Framework
from theframework.request import Request as Request
from theframework.response import Response as Response

__all__ = ["Framework", "Request", "Response", "version"]

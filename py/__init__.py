"""async_fs public interface – exported from compiled _async_fs.so"""
import sys
if sys.platform != "linux":
    raise ImportError("async-fs is Linux-only (requires io_uring)")

from importlib import import_module as _imp

_mod = _imp("._async_fs", __name__)
globals().update({k: getattr(_mod, k) for k in getattr(_mod, "__all__", dir(_mod))})

__version__ = "1.0.0"

# cleanup
del _imp, _mod

"""Process-owned read/write pins for the active content store.

The directory inode is the lock: reads never create a file. Kernel release on
process exit replaces guessed heartbeat/TTL expiry. Collection is nonblocking;
it cannot stop monitoring or an order while a lazy reader still owns content.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import functools
import os
from pathlib import Path

_EXCLUSIVE = ContextVar('exclusive_storage_root', default=None)


class StorageBusy(ValueError):
    pass


class Pin:
    def __init__(self, base, *, exclusive=False, wait=False):
        self.fd = None
        try:import fcntl
        except ImportError:
            if exclusive:raise StorageBusy('OS_STORAGE_LEASE_UNSUPPORTED')
            return  # Keep legacy reads/writes usable; physical GC is disabled.
        self.base = Path(base).absolute()
        if self.base.is_symlink(): raise ValueError('STORAGE_ROOT_SYMLINK_FORBIDDEN')
        if not self.base.exists(): return
        if _EXCLUSIVE.get() == str(self.base.resolve()): return
        fd = os.open(self.base, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | (0 if wait else fcntl.LOCK_NB))
        except BaseException as exc:
            os.close(fd)
            if isinstance(exc,BlockingIOError):raise StorageBusy('STORAGE_GENERATION_BUSY') from exc
            raise
        self.fd = fd

    def close(self):
        if self.fd is not None:
            os.close(self.fd); self.fd = None

    def __del__(self):
        self.close()


@contextmanager
def exclusive(base):
    pin = Pin(base, exclusive=True)
    token = _EXCLUSIVE.set(str(Path(base).resolve()))
    try: yield
    finally: _EXCLUSIVE.reset(token); pin.close()


def guarded(fn):
    """Hold a content pin across eager legacy decoding or object construction."""
    @functools.wraps(fn)
    def run(*args, **kwargs):
        import snapshot_store
        base = snapshot_store.root(kwargs.get('base')).parent.parent
        pin = Pin(base)
        try: return fn(*args, **kwargs)
        finally: pin.close()
    return run

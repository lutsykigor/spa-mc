"""CPU discovery and per-thread soft affinity (Linux and Windows; macOS has no thread-affinity API)."""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

# A Windows affinity mask covers one processor group (at most 64 logical CPUs).
WINDOWS_GROUP_SIZE = 64


def available_cpus() -> list[int]:
    if hasattr(os, 'sched_getaffinity'):
        return sorted(os.sched_getaffinity(0))
    count = os.process_cpu_count() or 1
    if sys.platform == 'win32':
        count = min(count, WINDOWS_GROUP_SIZE)
    return list(range(count))


def physical_cpus() -> list[int]:
    """One logical CPU per physical core (SMT siblings collapsed), Linux only; else all CPUs."""
    cpus = available_cpus()
    if not sys.platform.startswith('linux'):
        return cpus
    chosen, seen = [], set()
    for cpu in cpus:
        siblings = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list')
        try:
            key = siblings.read_text().strip()
        except OSError:
            key = str(cpu)
        if key not in seen:
            seen.add(key)
            chosen.append(cpu)
    return chosen


def pin_current_thread(cpu: int) -> bool:
    """Restrict the calling OS thread to `cpu`. Returns False where the OS has no such API (macOS)."""
    if hasattr(os, 'sched_setaffinity'):
        # On Linux pid 0 means the calling thread, not the whole process.
        os.sched_setaffinity(0, {cpu})
        return True
    if sys.platform == 'win32':
        _windows_set_thread_affinity(1 << cpu)
        return True
    return False


def current_thread_cpus() -> set[int] | None:
    if hasattr(os, 'sched_getaffinity'):
        return os.sched_getaffinity(0)
    if sys.platform == 'win32':
        # Windows has no getter: setting returns the previous mask, which is then restored.
        mask = _windows_set_thread_affinity((1 << len(available_cpus())) - 1)
        _windows_set_thread_affinity(mask)
        return {cpu for cpu in range(WINDOWS_GROUP_SIZE) if mask >> cpu & 1}
    return None


def _windows_set_thread_affinity(mask: int) -> int:
    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel32.GetCurrentThread.restype = ctypes.c_void_p
    kernel32.SetThreadAffinityMask.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    kernel32.SetThreadAffinityMask.restype = ctypes.c_size_t
    previous = kernel32.SetThreadAffinityMask(kernel32.GetCurrentThread(), mask)
    if previous == 0:
        raise ctypes.WinError(ctypes.get_last_error())
    return previous

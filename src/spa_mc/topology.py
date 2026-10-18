"""CPU discovery and per-thread soft affinity (Linux and Windows; macOS has no thread-affinity API)."""

from __future__ import annotations

import ctypes
import math
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


def cpu_budget() -> list[int]:
    """The CPUs workers should use: one per physical core, no more than the container's CPU quota allows."""
    cpus = physical_cpus()
    quota = cpu_quota()
    if quota is not None:
        cpus = cpus[: max(1, math.ceil(quota))]
    return cpus


CGROUP_ROOT = Path('/sys/fs/cgroup')
PROC_CGROUP = Path('/proc/self/cgroup')


def cpu_quota(cgroup_root: Path = CGROUP_ROOT, proc_cgroup: Path = PROC_CGROUP) -> float | None:
    """CPUs' worth of time the process's cgroup allows (v2 `cpu.max` or v1 CFS quota); None when unlimited or unknown.

    Python's own CPU counts ignore this, so a container limited to 2 CPUs on a 64-core host otherwise looks like 64.
    """
    try:
        entries = proc_cgroup.read_text().splitlines()
    except OSError:
        return None
    limits = []
    for entry in entries:
        hierarchy, controllers, path = entry.split(':', 2)
        if hierarchy == '0':
            limits += _cgroup_v2_limits(cgroup_root, path)
        elif 'cpu' in controllers.split(','):
            limits += _cgroup_v1_limits(cgroup_root / controllers, path)
    return min(limits) if limits else None


def _cgroup_v2_limits(root: Path, path: str) -> list[float]:
    limits = []
    for directory in _cgroup_dirs(root, path):
        try:
            quota, period = (directory / 'cpu.max').read_text().split()
        except OSError, ValueError:
            continue
        if quota != 'max':
            limits.append(int(quota) / int(period))
    return limits


def _cgroup_v1_limits(root: Path, path: str) -> list[float]:
    limits = []
    for directory in _cgroup_dirs(root, path):
        try:
            quota = int((directory / 'cpu.cfs_quota_us').read_text())
            period = int((directory / 'cpu.cfs_period_us').read_text())
        except OSError, ValueError:
            continue
        if quota > 0 and period > 0:
            limits.append(quota / period)
    return limits


def _cgroup_dirs(root: Path, path: str) -> list[Path]:
    """The process's cgroup and its ancestors; a limit set on any of them applies."""
    directory = root / path.lstrip('/')
    if not directory.is_dir():
        # Inside a container the host-side path is not mounted; the container's own cgroup is the root.
        return [root]
    dirs = [directory]
    while directory != root and root in directory.parents:
        directory = directory.parent
        dirs.append(directory)
    return dirs


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

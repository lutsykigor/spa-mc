from pathlib import Path

import pytest

from spa_mc import topology


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


@pytest.fixture
def cgroup(tmp_path: Path):
    root, proc = tmp_path / 'cgroup', tmp_path / 'proc_cgroup'
    root.mkdir()
    return root, proc


def test_cgroup_v2_quota_is_read_from_cpu_max(cgroup):
    root, proc = cgroup
    write(proc, '0::/\n')
    write(root / 'cpu.max', '200000 100000\n')
    assert topology.cpu_quota(root, proc) == 2.0


def test_cgroup_v2_without_a_limit_reports_none(cgroup):
    root, proc = cgroup
    write(proc, '0::/\n')
    write(root / 'cpu.max', 'max 100000\n')
    assert topology.cpu_quota(root, proc) is None


def test_the_tightest_limit_along_the_cgroup_path_wins(cgroup):
    root, proc = cgroup
    write(proc, '0::/app/web\n')
    write(root / 'app' / 'cpu.max', '150000 100000\n')
    write(root / 'app' / 'web' / 'cpu.max', 'max 100000\n')
    assert topology.cpu_quota(root, proc) == 1.5


def test_cgroup_v1_cfs_quota(cgroup):
    root, proc = cgroup
    write(proc, '4:cpu,cpuacct:/\n3:memory:/\n')
    write(root / 'cpu,cpuacct' / 'cpu.cfs_quota_us', '50000\n')
    write(root / 'cpu,cpuacct' / 'cpu.cfs_period_us', '100000\n')
    assert topology.cpu_quota(root, proc) == 0.5


def test_cgroup_v1_unlimited_quota(cgroup):
    root, proc = cgroup
    write(proc, '4:cpu,cpuacct:/\n')
    write(root / 'cpu,cpuacct' / 'cpu.cfs_quota_us', '-1\n')
    write(root / 'cpu,cpuacct' / 'cpu.cfs_period_us', '100000\n')
    assert topology.cpu_quota(root, proc) is None


def test_no_cgroup_information_means_no_quota(tmp_path):
    assert topology.cpu_quota(tmp_path, tmp_path / 'missing') is None


def test_budget_is_capped_by_the_quota_and_never_below_one(monkeypatch):
    monkeypatch.setattr(topology, 'physical_cpus', lambda: [0, 1, 2, 3, 4, 5])
    monkeypatch.setattr(topology, 'cpu_quota', lambda: 2.5)
    assert topology.cpu_budget() == [0, 1, 2]
    monkeypatch.setattr(topology, 'cpu_quota', lambda: 0.2)
    assert topology.cpu_budget() == [0]
    monkeypatch.setattr(topology, 'cpu_quota', lambda: None)
    assert topology.cpu_budget() == [0, 1, 2, 3, 4, 5]

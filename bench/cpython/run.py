"""Benchmark the CPython asyncio multi-core branch against unmodified CPython.

Every scenario starts `server.py` under the chosen interpreter in a fresh process and drives it from separate
load-generator processes (closed loop: each simulated client sends one request per connection, waits for the reply,
repeats). Scenarios are interleaved over several rounds and the median round is reported, so slow drift cannot
favour one of them. Before every scenario a calibration loop is timed on one core and on four at once; if it is
more than `--throttle-tolerance` slower than at start-up the CPU is assumed to be throttled, and the run rests.

usage:
  python run.py --python-base BASE_PYTHON --python-multicore BRANCH_PYTHON [--python-ft FREE_THREADED_PYTHON]
                [--workload cpu|echo|both] [--suite throughput|noisy|all] [--workers 4]
                [--rounds 3] [--duration 10] [--out results.json]
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SERVER = str(HERE / 'server.py')
WORKLOADS = {'cpu': 50_000, 'echo': 0}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--python-base', required=True, help='unmodified CPython (main) interpreter')
    parser.add_argument('--python-multicore', required=True, help='CPython built from the asyncio-multicore branch')
    parser.add_argument('--python-ft', help='optional free-threaded CPython, for the threads scenario')
    parser.add_argument('--workload', choices=['cpu', 'echo', 'both'], default='both')
    parser.add_argument('--suite', choices=['throughput', 'noisy', 'all'], default='all')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--duration', type=float, default=10.0)
    parser.add_argument('--clients', type=int, default=2, help='load-generator processes')
    parser.add_argument('--concurrency', type=int, default=32, help='connections in flight per load process')
    parser.add_argument('--cooldown', type=float, default=10.0)
    parser.add_argument('--throttle-tolerance', type=float, default=0.10)
    parser.add_argument('--only', help='run only scenarios whose name contains this text')
    parser.add_argument('--out', default=str(HERE / 'results.json'))
    args = parser.parse_args()

    scenarios = build_scenarios(args)
    if not scenarios:
        sys.exit('no scenarios selected')
    check_interpreters(args)
    baseline = calibrate()
    log(f'calibration {baseline * 1000:.1f} ms; {len(scenarios)} scenarios x {args.rounds} rounds')
    results: dict[str, list[dict]] = {name: [] for name, _ in scenarios}
    for round_no in range(args.rounds):
        for name, command in scenarios:
            factor = wait_until_cool(baseline, args.throttle_tolerance, args.cooldown)
            run = measure(command, args)
            run['calibration_factor'] = round(factor, 3)
            results[name].append(run)
            log(f'round {round_no + 1} {name:<44} {summary(run)}  (cpu x{factor:.2f})')
            time.sleep(args.cooldown)

    report = {
        'machine': machine_info(),
        'interpreters': {key: version_of(getattr(args, key)) for key in ('python_base', 'python_multicore', 'python_ft')
                         if getattr(args, key)},
        'settings': {k: v for k, v in vars(args).items() if not k.startswith('python') and k != 'out'},
        'scenarios': {name: {'median': median_run(runs), 'runs': runs} for name, runs in results.items()},
    }
    out = Path(args.out)
    out.write_text(json.dumps(report, indent=2))
    log(f'wrote {out}')
    print_table(report)


def build_scenarios(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    n = str(args.workers)
    base, multi, ft = args.python_base, args.python_multicore, args.python_ft
    workloads = ['cpu', 'echo'] if args.workload == 'both' else [args.workload]
    scenarios = []
    if args.suite in ('throughput', 'all'):
        for workload in workloads:
            work = ['--work', str(WORKLOADS[workload])]
            scenarios += [
                (f'{workload}: main, single loop', [base, SERVER, 'single', '1', *work]),
                (f'{workload}: branch, single loop (default path)', [multi, SERVER, 'single', '1', *work]),
                (f'{workload}: main, prefork {n} processes', [base, SERVER, 'prefork', n, *work]),
                (f'{workload}: branch, workers={n}', [multi, SERVER, 'workers', n, *work]),
                (f'{workload}: branch, workers=auto', [multi, SERVER, 'workers', 'auto', *work]),
            ]
            if ft:
                scenarios.append((f'{workload}: free-threaded, {n} threads', [ft, SERVER, 'threads', n, *work]))
    if args.suite in ('noisy', 'all'):
        # Worker 0 loses its CPU 60 ms in every 250 ms: does load-aware routing protect the tail?
        noisy = [multi, SERVER, 'workers', n, '--noisy', '--work', str(WORKLOADS['cpu'])]
        scenarios += [
            (f'noisy: branch, workers={n}, round_robin', [*noisy, '--routing', 'round_robin']),
            (f'noisy: branch, workers={n}, least_loaded', [*noisy, '--routing', 'least_loaded']),
        ]
    if args.only:
        scenarios = [(name, command) for name, command in scenarios if args.only in name]
    return scenarios


def check_interpreters(args: argparse.Namespace) -> None:
    probe = 'import asyncio, inspect; print("workers" in inspect.signature(asyncio.start_server).parameters)'
    has_workers = subprocess.run([args.python_multicore, '-c', probe], capture_output=True, text=True).stdout.strip()
    if has_workers != 'True':
        sys.exit(f'{args.python_multicore} has no asyncio.start_server(workers=...): build the asyncio-multicore branch')


def version_of(python: str) -> str:
    code = 'import sys, sysconfig; print(sys.version, "| free-threaded" if sysconfig.get_config_var("Py_GIL_DISABLED") else "")'
    return subprocess.run([python, '-c', code], capture_output=True, text=True).stdout.strip()


def calibrate(parallel: int = 4) -> float:
    """Slower of: best single-core loop time, and the same loop on `parallel` processes at once."""
    single = min(_spin() for _ in range(3))
    with multiprocessing.get_context('spawn').Pool(parallel) as pool:
        pool.map(_spin, range(parallel))
        all_core = max(pool.map(_spin, range(parallel)))
    return max(single, all_core)


def _spin(_: int = 0) -> float:
    started = time.perf_counter()
    total = 0
    for i in range(2_000_000):
        total += i * i
    return time.perf_counter() - started


def wait_until_cool(baseline: float, tolerance: float, rest: float) -> float:
    factor = 1.0
    for _ in range(6):
        factor = calibrate() / baseline
        if factor <= 1 + tolerance:
            return factor
        log(f'  calibration x{factor:.2f} slower than at start: CPU looks throttled, resting {rest * 3:.0f}s')
        time.sleep(rest * 3)
    log('  still slow after resting; measuring anyway (see calibration_factor)')
    return factor


def measure(command: list[str], args: argparse.Namespace) -> dict:
    server = subprocess.Popen(command, cwd=HERE, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr,
                              text=True)
    try:
        line = server.stdout.readline()
        if not line.startswith('READY'):
            raise RuntimeError(f'server did not start: {command}')
        port = int(line.split()[1])
        with multiprocessing.get_context('spawn').Pool(args.clients) as pool:
            parts = pool.starmap(load_client, [(port, args.concurrency, args.duration)] * args.clients)
    finally:
        server.stdin.close()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    latencies = sorted(lat for part in parts for lat in part['latencies'])
    return {
        'ok': len(latencies),
        'errors': sum(part['errors'] for part in parts),
        'rps': round(len(latencies) / args.duration, 1),
        'p50_ms': pct(latencies, 0.50),
        'p99_ms': pct(latencies, 0.99),
        'p999_ms': pct(latencies, 0.999),
        'max_ms': round(latencies[-1] * 1000, 2) if latencies else None,
    }


def load_client(port: int, concurrency: int, duration: float) -> dict:
    return asyncio.run(_load(port, concurrency, duration))


async def _load(port: int, concurrency: int, duration: float) -> dict:
    stats = {'latencies': [], 'errors': 0}
    deadline = time.perf_counter() + duration

    async def client() -> None:
        while time.perf_counter() < deadline:
            started = time.perf_counter()
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
                writer.write(b'go\n')
                await writer.drain()
                reply = await reader.read()
                writer.close()
            except OSError:
                stats['errors'] += 1
                await asyncio.sleep(0.001)
                continue
            if reply:
                stats['latencies'].append(time.perf_counter() - started)
            else:
                stats['errors'] += 1

    await asyncio.gather(*(client() for _ in range(concurrency)))
    return stats


def pct(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    return round(sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))] * 1000, 2)


def median_run(runs: list[dict]) -> dict:
    middle = statistics.median(run['rps'] for run in runs)
    return min(runs, key=lambda run: abs(run['rps'] - middle))


def summary(run: dict) -> str:
    return (f"{run['rps']:>9.0f} req/s  p50 {run['p50_ms']} ms  p99 {run['p99_ms']} ms  "
            f"p99.9 {run['p999_ms']} ms  errors {run['errors']}")


def machine_info() -> dict:
    return {'platform': platform.platform(), 'processor': platform.processor(), 'cpus': os.cpu_count(),
            'harness_python': sys.version.split()[0]}


def print_table(report: dict) -> None:
    print('\n| scenario | req/s | p50 ms | p99 ms | p99.9 ms | errors | cpu factor |')
    print('|---|---:|---:|---:|---:|---:|---:|')
    for name, data in report['scenarios'].items():
        m = data['median']
        print(f"| {name} | {m['rps']:.0f} | {m['p50_ms']} | {m['p99_ms']} | {m['p999_ms']} | {m['errors']} | "
              f"{m['calibration_factor']} |")


def log(message: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {message}', flush=True)


if __name__ == '__main__':
    main()

"""Throttle-aware benchmark harness.

Each scenario starts `server.py` in a fresh process and drives it from separate load-generator
processes. Before every scenario a fixed calibration loop is timed on one core and on four at once; if it is more than
`--throttle-tolerance` slower than the baseline taken at start-up the CPU is assumed to be thermally
throttled, so the harness rests and re-checks before measuring. Scenarios are interleaved over
several rounds and the median round is reported, so slow drift cannot favour one model.
Clients back off 5 ms after a shed (BUSY) reply, as a well-behaved client would.

usage: run_bench.py [--suite throughput|noisy|all] [--rounds 3] [--duration 8] [--out results.json]
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--suite', choices=['throughput', 'noisy', 'hog', 'all'], default='all')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--duration', type=float, default=8.0)
    parser.add_argument('--work', type=int, default=50_000)
    parser.add_argument('--clients', type=int, default=2)
    parser.add_argument('--concurrency', type=int, default=32)
    parser.add_argument('--cooldown', type=float, default=15.0)
    parser.add_argument('--throttle-tolerance', type=float, default=0.10)
    parser.add_argument('--baseline-ms', type=float, help='cold-machine calibration time; measured now if omitted')
    parser.add_argument('--hogs', type=int, default=os.cpu_count(), help='busy-loop processes in hog: scenarios')
    parser.add_argument('--only', help='run only scenarios whose name contains this text')
    parser.add_argument('--python-ft', help='free-threaded interpreter for the threads-ft scenario')
    parser.add_argument('--out', default=str(HERE.parent / 'results' / 'results.json'))
    args = parser.parse_args()

    scenarios = build_scenarios(args)
    baseline = args.baseline_ms / 1000 if args.baseline_ms else calibrate()
    log(f'calibration baseline {baseline * 1000:.1f} ms; {len(scenarios)} scenarios x {args.rounds} rounds')
    results: dict[str, list[dict]] = {name: [] for name, _ in scenarios}
    for round_no in range(args.rounds):
        for name, command in scenarios:
            factor = wait_until_cool(baseline, args.throttle_tolerance, args.cooldown)
            run = measure(command, args, hogs=args.hogs if name.startswith('hog:') else 0)
            run['calibration_factor'] = round(factor, 3)
            results[name].append(run)
            log(f'round {round_no + 1} {name:<28} {summary(run)}  (cpu x{factor:.2f})')
            time.sleep(args.cooldown)

    report = {
        'machine': machine_info(),
        'settings': {k: v for k, v in vars(args).items() if k not in ('out', 'python_ft')}
        | {'python_ft': Path(args.python_ft).name if args.python_ft else None},
        'calibration_baseline_ms': round(baseline * 1000, 2),
        'scenarios': {name: {'median': median_run(runs), 'runs': runs} for name, runs in results.items()},
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log(f'wrote {out}')
    print_table(report)


def build_scenarios(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    py, n = sys.executable, str(args.workers)
    server = str(HERE / 'server.py')
    scenarios = []
    if args.suite in ('throughput', 'all'):
        scenarios += [
            ('asyncio-single-loop', [py, server, 'single', '1']),
            (f'asyncio+threads({n})-gil', [py, server, 'threads', n]),
            (f'prefork-processes({n})', [py, server, 'prefork', n]),
            (f'spa-mc({n})', [py, server, 'spa-mc', n]),
        ]
        if args.python_ft:
            scenarios.append((f'asyncio+threads({n})-free-threaded', [args.python_ft, server, 'threads', n]))
    if args.suite in ('noisy', 'all'):
        scenarios += [
            (
                f'noisy:spa-mc({n})-roundrobin-novalve',
                [py, server, 'spa-mc', n, '--noisy', '--routing', 'round_robin', '--no-limiter'],
            ),
            (f'noisy:spa-mc({n})-leastloaded-valve', [py, server, 'spa-mc', n, '--noisy']),
        ]
    if args.suite == 'hog':
        # Competing processes on every core: does parking workers whose core is taken pay off?
        auto = [py, server, 'spa-mc', 'auto', '--adapt-interval', '0.5']
        scenarios += [
            ('hog:spa-mc(auto)-parking', [*auto, '--park']),
            ('hog:spa-mc(auto)-no-parking', auto),
        ]
    if args.only:
        scenarios = [(name, cmd) for name, cmd in scenarios if args.only in name]
    return scenarios


def calibrate(parallel: int = 4) -> float:
    """Slower of: best single-core loop time, and the same loop on `parallel` processes at once."""
    single = min(_spin() for _ in range(3))
    with multiprocessing.get_context('spawn').Pool(parallel) as pool:
        pool.map(_spin, range(parallel))
        all_core = max(pool.map(_spin, range(parallel)))
    return max(single, all_core)


def _hog(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pass


def _spin(_: int = 0) -> float:
    started = time.perf_counter()
    total = 0
    for i in range(2_000_000):
        total += i * i
    return time.perf_counter() - started


def wait_until_cool(baseline: float, tolerance: float, rest: float) -> float:
    for attempt in range(6):
        factor = calibrate() / baseline
        if factor <= 1 + tolerance:
            return factor
        log(f'  calibration x{factor:.2f} slower than baseline: CPU looks throttled, resting {rest * 3:.0f}s')
        time.sleep(rest * 3)
    log('  still throttled after resting; measuring anyway (flagged by calibration_factor)')
    return factor


def measure(command: list[str], args: argparse.Namespace, hogs: int = 0) -> dict:
    env = {**os.environ, 'PYTHONPATH': str(HERE.parent / 'src')}
    server = subprocess.Popen(
        command, cwd=HERE, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr, text=True, env=env
    )
    try:
        line = server.stdout.readline()
        if not line.startswith('READY'):
            raise RuntimeError(f'server did not start: {command}')
        port = int(line.split()[1])
        context = multiprocessing.get_context('spawn')
        hog_processes = [context.Process(target=_hog, args=(args.duration + 3,)) for _ in range(hogs)]
        for hog in hog_processes:
            hog.start()
        # Let the engine's adaptation see the competition before measuring.
        time.sleep(2.0 if hogs else 0.0)
        with context.Pool(args.clients) as pool:
            parts = pool.starmap(load_client, [(port, args.concurrency, args.duration, args.work)] * args.clients)
        for hog in hog_processes:
            hog.terminate()
            hog.join()
    finally:
        server.stdin.close()
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    latencies = sorted(lat for part in parts for lat in part['latencies'])
    ok = len(latencies)
    return {
        'ok': ok,
        'shed': sum(part['shed'] for part in parts),
        'errors': sum(part['errors'] for part in parts),
        'rps': round(ok / args.duration, 1),
        'p50_ms': pct(latencies, 0.50),
        'p99_ms': pct(latencies, 0.99),
        'p999_ms': pct(latencies, 0.999),
        'max_ms': round(latencies[-1] * 1000, 2) if latencies else None,
    }


def load_client(port: int, concurrency: int, duration: float, work: int) -> dict:
    return asyncio.run(_load(port, concurrency, duration, work))


async def _load(port: int, concurrency: int, duration: float, work: int) -> dict:
    stats = {'latencies': [], 'shed': 0, 'errors': 0}
    deadline = time.perf_counter() + duration
    payload = f'{work}\n'.encode()

    async def one_connection_at_a_time() -> None:
        while time.perf_counter() < deadline:
            started = time.perf_counter()
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
                writer.write(payload)
                await writer.drain()
                reply = await reader.read()
                writer.close()
            except OSError:
                stats['errors'] += 1
                await asyncio.sleep(0.001)
                continue
            if reply == b'BUSY\n':
                stats['shed'] += 1
                await asyncio.sleep(0.005)
            elif reply:
                stats['latencies'].append(time.perf_counter() - started)
            else:
                stats['errors'] += 1

    await asyncio.gather(*(one_connection_at_a_time() for _ in range(concurrency)))
    return stats


def pct(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    return round(sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))] * 1000, 2)


def median_run(runs: list[dict]) -> dict:
    return min(runs, key=lambda run: abs(run['rps'] - statistics.median(r['rps'] for r in runs)))


def summary(run: dict) -> str:
    return (
        f'{run["rps"]:>8.0f} req/s  p50 {run["p50_ms"]} ms  p99 {run["p99_ms"]} ms  '
        f'p99.9 {run["p999_ms"]} ms  shed {run["shed"]}  err {run["errors"]}'
    )


def machine_info() -> dict:
    info = {'platform': platform.platform(), 'python': sys.version.split()[0], 'cpus': os.cpu_count()}
    if sys.platform == 'win32':
        info['processor'] = platform.processor()
    if sys.platform == 'darwin':
        for key in ('machdep.cpu.brand_string', 'hw.perflevel0.physicalcpu', 'hw.perflevel1.physicalcpu'):
            info[key] = subprocess.run(['sysctl', '-n', key], capture_output=True, text=True).stdout.strip()
    return info


def print_table(report: dict) -> None:
    print('\n| scenario | req/s | p50 ms | p99 ms | p99.9 ms | shed | errors | cpu factor |')
    print('|---|---:|---:|---:|---:|---:|---:|---:|')
    for name, data in report['scenarios'].items():
        m = data['median']
        print(
            f'| {name} | {m["rps"]:.0f} | {m["p50_ms"]} | {m["p99_ms"]} | {m["p999_ms"]} | {m["shed"]} | '
            f'{m["errors"]} | {m["calibration_factor"]} |'
        )


def log(message: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {message}', flush=True)


if __name__ == '__main__':
    if sys.argv[1:] == ['--calibrate']:
        print(f'{calibrate() * 1000:.1f}')
    else:
        main()

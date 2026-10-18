"""Per-worker health records in one shared buffer, readable across interpreters.

Each worker owns one `STRIDE`-byte slot and is its only writer. A reader in another
interpreter runs truly in parallel (separate GIL), so a plain multi-field read can be
torn; every slot is guarded by a sequence counter (seqlock): the writer makes it odd
before writing and even after, and the reader retries until it sees the same even value
on both sides of its copy. The values are advisory routing hints, never used for
correctness, so a best-effort seqlock without explicit memory fences is acceptable.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from enum import IntEnum

# 128 bytes covers the cache-line size of both x86-64 (64) and Apple silicon (128).
STRIDE = 128

_SEQ = struct.Struct('<Q')
_BODY = struct.Struct('<IIIIQQQQddddd')
assert _SEQ.size + _BODY.size <= STRIDE


class WorkerState(IntEnum):
    STARTING = 0
    READY = 1
    STOPPING = 2
    STOPPED = 3
    FAILED = 4


@dataclass(frozen=True, slots=True)
class WorkerStats:
    worker_id: int
    state: WorkerState
    in_flight: int
    limit: int
    core: int
    received: int
    completed: int
    shed: int
    stalls: int
    rtt_ms: float
    rtt_noload_ms: float
    loop_lag_ms: float
    max_loop_lag_ms: float
    heartbeat: float

    @property
    def heartbeat_age(self) -> float:
        return time.monotonic() - self.heartbeat


_MAX_STATE = max(WorkerState)


def allocate(workers: int) -> memoryview:
    return memoryview(bytearray(STRIDE * workers))


class SlotWriter:
    def __init__(self, buffer: memoryview, worker_id: int) -> None:
        self._buf = buffer
        self._offset = worker_id * STRIDE

    def publish(
        self,
        *,
        state: WorkerState,
        in_flight: int,
        limit: int,
        core: int,
        received: int,
        completed: int,
        shed: int,
        stalls: int,
        rtt: float,
        rtt_noload: float,
        loop_lag: float,
        max_loop_lag: float,
    ) -> None:
        (seq,) = _SEQ.unpack_from(self._buf, self._offset)
        _SEQ.pack_into(self._buf, self._offset, seq + 1)
        _BODY.pack_into(
            self._buf,
            self._offset + _SEQ.size,
            int(state),
            in_flight,
            limit,
            core & 0xFFFFFFFF,
            received,
            completed,
            shed,
            stalls,
            rtt * 1000.0,
            0.0 if rtt_noload == float('inf') else rtt_noload * 1000.0,
            loop_lag * 1000.0,
            max_loop_lag * 1000.0,
            time.monotonic(),
        )
        _SEQ.pack_into(self._buf, self._offset, seq + 2)


def read(buffer: memoryview, worker_id: int) -> WorkerStats:
    stats = try_read(buffer, worker_id, max_retries=100_000)
    if stats is None:
        raise TimeoutError(f'telemetry slot {worker_id} never became consistent')
    return stats


def try_read(buffer: memoryview, worker_id: int, max_retries: int = 64) -> WorkerStats | None:
    """None when the writer stays mid-update (e.g. descheduled by the OS) for the whole retry budget."""
    offset = worker_id * STRIDE
    for _ in range(max_retries):
        (before,) = _SEQ.unpack_from(buffer, offset)
        if before & 1:
            continue
        body = _BODY.unpack_from(buffer, offset + _SEQ.size)
        (after,) = _SEQ.unpack_from(buffer, offset)
        # The state check also rejects a torn body that slipped past the counter on weakly ordered CPUs.
        if before == after and body[0] <= _MAX_STATE:
            return _to_stats(worker_id, body)
    return None


def _to_stats(worker_id: int, body: tuple) -> WorkerStats:
    state, in_flight, limit, core, received, completed, shed, stalls, rtt, noload, lag, max_lag, beat = body
    return WorkerStats(
        worker_id=worker_id,
        state=WorkerState(state),
        in_flight=in_flight,
        limit=limit,
        core=core if core != 0xFFFFFFFF else -1,
        received=received,
        completed=completed,
        shed=shed,
        stalls=stalls,
        rtt_ms=rtt,
        rtt_noload_ms=noload,
        loop_lag_ms=lag,
        max_loop_lag_ms=max_lag,
        heartbeat=beat,
    )

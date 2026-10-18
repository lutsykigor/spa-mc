"""Per-worker health records in one shared buffer, readable across interpreters.

Each worker owns one `STRIDE`-byte slot and is its only writer. A reader in another
interpreter runs truly in parallel (separate GIL), so a copy can catch a record half
written. Python cannot issue memory fences, so a sequence counter (seqlock) is not
enough on weakly ordered CPUs: one was observed to tear on Apple silicon. Instead every
record carries a CRC32 of its fields, is written with one copy, and is read with one
copy that is accepted only if the checksum matches.
"""

from __future__ import annotations

import struct
import time
import zlib
from dataclasses import dataclass
from enum import IntEnum

# 128 bytes covers the cache-line size of both x86-64 (64) and Apple silicon (128).
STRIDE = 128

_CRC = struct.Struct('<I')
_BODY = struct.Struct('<IIIIIQQQQddddd')
RECORD = _CRC.size + _BODY.size
assert RECORD <= STRIDE
_NEVER_WRITTEN = bytes(RECORD)


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
    queued: int
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
        queued: int = 0,
    ) -> None:
        body = _BODY.pack(
            int(state),
            in_flight,
            queued,
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
        self._buf[self._offset : self._offset + RECORD] = _CRC.pack(zlib.crc32(body)) + body


def read(buffer: memoryview, worker_id: int) -> WorkerStats:
    stats = try_read(buffer, worker_id, max_retries=100_000)
    if stats is None:
        raise TimeoutError(f'telemetry slot {worker_id} never became consistent')
    return stats


def try_read(buffer: memoryview, worker_id: int, max_retries: int = 64) -> WorkerStats | None:
    """None when no intact copy was seen within the retry budget (e.g. a writer frozen mid-copy)."""
    offset = worker_id * STRIDE
    for _ in range(max_retries):
        record = bytes(buffer[offset : offset + RECORD])
        if record == _NEVER_WRITTEN:
            return _to_stats(worker_id, _BODY.unpack(record[_CRC.size :]))
        (crc,) = _CRC.unpack_from(record)
        body = record[_CRC.size :]
        if zlib.crc32(body) == crc:
            return _to_stats(worker_id, _BODY.unpack(body))
    return None


def _to_stats(worker_id: int, body: tuple) -> WorkerStats:
    state, in_flight, queued, limit, core, received, completed, shed, stalls, rtt, noload, lag, max_lag, beat = body
    return WorkerStats(
        worker_id=worker_id,
        state=WorkerState(state),
        in_flight=in_flight,
        queued=queued,
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

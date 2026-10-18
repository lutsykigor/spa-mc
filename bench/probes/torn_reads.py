"""Shows why the shared telemetry slab needs checksummed records.

A subinterpreter (own GIL, so truly parallel) keeps rewriting two records whose fields must all be equal:
slot 0 with a plain multi-field write (as the source PoC does), slot 1 through spa_mc.telemetry (checksummed).
The main interpreter reads both and counts reads whose fields disagree.

usage: python torn_reads.py [seconds]
"""

import struct
import sys
import threading
from concurrent import interpreters

from spa_mc import telemetry

RAW = struct.Struct('<QQQQQQQQ')
WRITER = """
import struct, time
from spa_mc import telemetry
from spa_mc.telemetry import WorkerState
raw = struct.Struct('<QQQQQQQQ')
writer = telemetry.SlotWriter(slab, 1)
deadline = time.monotonic() + duration
k = 0
while time.monotonic() < deadline:
    k += 1
    raw.pack_into(slab, 0, *([k] * 8))
    writer.publish(state=WorkerState.READY, in_flight=k % 1000, limit=k % 1000, core=k % 1000, received=k,
                   completed=k, shed=k, stalls=k, rtt=0.0, rtt_noload=0.0, loop_lag=0.0, max_loop_lag=0.0)
"""


def main(duration: float) -> None:
    slab = telemetry.allocate(2)
    interp = interpreters.create()
    interp.prepare_main(slab=slab, duration=duration)
    thread = threading.Thread(target=interp.exec, args=(WRITER,))
    thread.start()
    raw_reads = raw_torn = checked_reads = checked_torn = 0
    while thread.is_alive():
        values = RAW.unpack_from(slab, 0)
        raw_reads += 1
        raw_torn += len(set(values)) != 1
        stats = telemetry.try_read(slab, 1)
        if stats is not None:
            checked_reads += 1
            checked_torn += len({stats.received, stats.completed, stats.shed, stats.stalls}) != 1
    thread.join()
    interp.close()
    print(f'plain multi-field read : {raw_torn} torn of {raw_reads} reads')
    print(f'checksummed read       : {checked_torn} torn of {checked_reads} reads')


if __name__ == '__main__':
    main(float(sys.argv[1]) if len(sys.argv) > 1 else 2.0)

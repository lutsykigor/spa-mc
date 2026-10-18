"""Shows why the shared telemetry slab needs a seqlock: a parallel reader can see half-written records.

A subinterpreter (own GIL, so truly parallel) rewrites one record whose fields must all be equal.
The main interpreter reads it raw (as the source PoC does) and via spa_mc.telemetry.read (seqlock).
"""

import struct
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


def main(duration: float = 2.0) -> None:
    slab = telemetry.allocate(2)
    interp = interpreters.create()
    interp.prepare_main(slab=slab, duration=duration)
    thread = threading.Thread(target=interp.exec, args=(WRITER,))
    thread.start()
    raw_reads = raw_torn = seq_reads = seq_torn = 0
    while thread.is_alive():
        values = RAW.unpack_from(slab, 0)
        raw_reads += 1
        raw_torn += len(set(values)) != 1
        stats = telemetry.read(slab, 1)
        seq_reads += 1
        seq_torn += len({stats.received, stats.completed, stats.shed, stats.stalls}) != 1
    thread.join()
    interp.close()
    print(f'raw pack/unpack : {raw_torn} torn of {raw_reads} reads')
    print(f'seqlock read    : {seq_torn} torn of {seq_reads} reads')


if __name__ == '__main__':
    main()

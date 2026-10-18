import os
import sys
import threading
from concurrent import interpreters as I

print(sys.version.split()[0], 'cpus', os.cpu_count(), 'proc affinity', sorted(os.sched_getaffinity(0)))
# per-thread pinning from inside a subinterpreter
res = I.create_queue()
its = []


def worker(core):
    it = I.create()
    it.prepare_main(core=core, res=res)
    it.exec(
        'import os, threading\n'
        'os.sched_setaffinity(0, {core})\n'
        'res.put((core, threading.get_native_id(), tuple(sorted(os.sched_getaffinity(0)))))'
    )
    its.append(it)


ts = [threading.Thread(target=worker, args=(c,)) for c in range(3)]
[t.start() for t in ts]
[t.join() for t in ts]
for _ in range(3):
    print('  subinterp thread pinned:', res.get())
print('  main thread affinity unchanged:', sorted(os.sched_getaffinity(0)))

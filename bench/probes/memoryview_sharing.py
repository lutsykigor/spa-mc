import time
from concurrent import interpreters as I

buf = bytearray(64 * 1024 * 1024)
mv = memoryview(buf)
q = I.create_queue()
interp = I.create()
interp.prepare_main(q=q)
t0 = time.perf_counter()
q.put(mv)
interp.exec("m = q.get(); print('sub got', type(m).__name__, len(m), m.readonly); m[0] = 0x41; m[-1] = 0x42")
print(
    'put+get+write 64MiB:',
    round((time.perf_counter() - t0) * 1000, 2),
    'ms; parent sees',
    buf[0],
    buf[-1],
    '(zero-copy if 65 66)',
)
# prepare_main with memoryview
try:
    interp.prepare_main(mv2=memoryview(buf))
    interp.exec('mv2[1] = 0x43')
    print('prepare_main memoryview shared:', buf[1] == 0x43)
except Exception as e:
    print('prepare_main memoryview:', type(e).__name__, e)
# bytearray / dict
for obj in (bytearray(b'x'), {'a': 1}, [1, 2]):
    try:
        q.put(obj)
        interp.exec("o = q.get(); print('  got', type(o).__name__)")
    except Exception as e:
        print('  put', type(obj).__name__, '->', type(e).__name__, str(e)[:80])
print(
    'is_shareable(memoryview):',
    I.is_shareable(mv),
    ' bytearray:',
    I.is_shareable(bytearray()),
    ' int:',
    I.is_shareable(3),
)
interp.close()

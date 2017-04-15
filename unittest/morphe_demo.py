# coding: utf-8
import sys
sys.path.append("..")
from Morphe.Atomos import Atomos
from Morphe.Morphe import Morphe

import time
import threading
import queue
import functools
import asyncio

def loop_run_forever(lp):
    lp.run_forever()

def get_from_atomos(q):
    count = 0
    while True:
        count += 1
        data = q.get()
        print("{}: get_from_atomos Got: ".format(count), data)

try:
    loop = asyncio.get_event_loop()
except:
    loop = asyncio.new_event_loop()
t = threading.Thread(target=functools.partial(loop_run_forever, loop), daemon=True)
t.start()

q = queue.Queue()
main = threading.Thread(target=functools.partial(get_from_atomos, q))
main.start()
print("Main 开启")

t = list()
for i in range(0, 3000):
    t.append(i)
    # time.sleep(0.001)

# 生成三个数据源
a1 = Atomos([{"time": t[i]} for i in range(0, 20, 1)], name="a1", buffer_size=1, time_column="time", loop=loop)
a2 = Atomos([{"time": t[i]} for i in range(0, 20, 2)], name="a2", buffer_size=1, time_column="time", loop=loop)
a3 = Atomos([{"time": t[i]} for i in range(0, 20, 3)], name="a3", buffer_size=1, time_column="time", loop=loop)

# 用一个Morphe订阅它们
m1 = Morphe(name="m1", buffer_size=1, time_column="time", loop=loop)
m1.subscribe(a1)
m1.subscribe(a2)
# m1 订阅了a1, a2

# m1.start()

m2 = Morphe(name="m2", buffer_size=1, time_column="time", loop=loop)
m2.subscribe(a2)
m2.subscribe(a3)
# m2 订阅了a2, a3

m3 = Morphe(sync_queue=q, buffer_size=1, name="m3", time_column="time", loop=loop)
m3.subscribe(m1)
m3.subscribe(m2)
# 由于subscribe接收的类型是Atomos，Morphe是Atomos的子类，所以
# m1, m2可以作为一个整体被m3订阅
# 下面就是m3作为Iterator按"time"顺序输出结果

# 注意：数据并不会全部输出的，当上游中同时有数据时候才会排序输出，当任何一条队列为空时，无法排序，就无法输出
m3.start()

main.join()

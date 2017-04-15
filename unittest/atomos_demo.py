# coding: utf-8
import sys
sys.path.append("..")
from Morphe.Atomos import Atomos
from Morphe.Morphe import Morphe
import asyncio
import time
import threading
import queue
import functools


def get_from_atomos(q):
    count = 0
    while True:
        count += 1
        data = q.get()
        print("{}: get_from_atomos Got: ".format(count), data)

q = queue.Queue()
main = threading.Thread(target=functools.partial(get_from_atomos, q))
main.start()
print("Main 开启")

t = list()
for i in range(0, 10000):
    t.append(i)


# 生成数据源a1
a1 = Atomos([{"time": t[i]} for i in range(0, 1000, 1)], name="List", buffer_size=10000)

# 生成数据源a2，订阅a1，且a2是终端，异步结果同步至q队列
a2 = Atomos(a1, sync_queue=q, name="Atomos", buffer_size=10)
a2.start()

main.join()

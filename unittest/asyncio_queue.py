# coding: utf-8
"""
决定将queue.Queue换成asyncio.Queue前试写的demo
"""
import asyncio
from asyncio import Queue
import queue
import random
import threading
import functools


@asyncio.coroutine
def queue_feeder(q):
    for i in range(0, 100):
        print("Put: ", i)
        yield from q.put(i)
        yield from asyncio.sleep(random.random()*3)


@asyncio.coroutine
def async_get(q):
    while True:
        print("Start get: ...")
        data = yield from q.get()
        print("Get: ", data)


@asyncio.coroutine
def async_get_sync(q, out):
    while True:
        print("Start get: ...")
        data = yield from q.get()
        out.put(data)


def get(q):
    import time
    while True:
        print("Start get from sync queue: ...")
        data = q.get()
        print("Sync get", data)

if __name__ == "__main__":
    # q1, q2是两个异步队列
    q1 = Queue()
    q2 = Queue()

    # 做一个同步队列用于将上游所有的异步消息同步化
    out = queue.Queue()
    t = threading.Thread(target=functools.partial(get, out), daemon=True)
    t.start()

    # 开启一个协程池， 异步执行q1, q2的异步生产和消费，消费过程是将数据丢进out这个同步队列以同步化
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*[queue_feeder(q1), queue_feeder(q2), async_get_sync(q1, out), async_get_sync(q2, out)]))

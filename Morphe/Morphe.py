# coding: utf-8
import asyncio
from queue import Queue
import random
import time
import bisect
from .Atomos import Atomos
from collections import defaultdict
from asyncio.queues import QueueEmpty
import copy


class Morphe(Atomos):
    """
    Morphe是希腊文中“形态”的意思，英文词根Morph-来源于此。
    
    A morphe subscribes morphes
    
    The accepted data source (which is usually another Morphe type), is supposed to be sorted in the order of "early first"
    
    self.__data_time: initiate as None, indicates the earliest timestamp of data, will change during iteration.
    self.__sorted_upstream: initiate as a empty list
    
    """
    def __init__(self, name, time_column="time", buffer_size=0, sync_queue=None):
        super().__init__(name=name, time_column=time_column, buffer_size=buffer_size, sync_queue=sync_queue)

        # 由于父类Atomos设置了self.__next = self.get，这里要替换掉
        self.__next = self.next
        self.buffer_size = buffer_size
        self.__sorted_upstream_buffer_queue = list()  # 排序后的数据源列表
        self.__sorted_time = list()  # 用于存放已经排序好的时间
        self.__sorted_data = list()  # 与__sorted_time对应的数据
        self.__sorted_name = list()
        self.upstream_buffer_queue = dict()  # 用来接收上游的数据
        self.empty_queue = defaultdict(asyncio.Queue)  # 存放那些上次取出内容为空的Queue, { atomos.name: asyncio.Queue}

    def subscribe(self, atomos):
        """
        订阅数据源
        :param atomos 
        :return: 
        """
        self.upstream_buffer_queue[atomos.name] = asyncio.Queue(maxsize=self.buffer_size)
        atomos.listener.append(self.upstream_buffer_queue[atomos.name])
        self.upstream[atomos.name] = atomos
        print("{}用{}订阅了{}".format(self.name, self.upstream_buffer_queue[atomos.name], atomos.name))

    def start(self):
        """
        ** NOT thread safe **
        :return: 
        """
        self.prepare()

        # 将async_next()加入协程池
        # TODO: not thread safe
        # 开启上游Atomos
        for name, atomos in self.upstream.items():
            atomos.start()

        # 开启本Atomos
        if not self.started:
            self.started = True
            asyncio.run_coroutine_threadsafe(self.async_next(), self._loop)
            print("Start: {}, {}".format(self.name, self.async_next()))

    def prepare(self):
        """
        将全部接收队列丢进empty_queue
        :return: 
        """
        for k, q in self.upstream_buffer_queue.items():
            print("{} set empty_queue {} ".format(self.name, k))
            self.empty_queue[k] = q
        print("{} prepare data DONE: {}".format(self.name, self.empty_queue))

    @asyncio.coroutine
    def next(self):
        """
        Overridden
        :return: 
        """
        # print("{} next is called. self.empty_queue:{}".format(self.name, self.empty_queue))
        # 先扫一遍empty_queue的队列，如果有新数据，进行插排
        keys = list(self.empty_queue.keys())
        # print("Keys", keys)
        for k in keys:
            q = self.empty_queue[k]
            # print("q", q)
            try:
                data = q.get_nowait()
                t = data[self.upstream[k].time_column]           # 取出这条数据的时间戳
                pos = bisect.bisect_left(self.__sorted_time, t)  # 判断插入位置
                self.__sorted_name.insert(pos, k)                                   #
                self.__sorted_time.insert(pos, t)                                   # 时间插入self.__sorted_time
                self.__sorted_data.insert(pos, data)                                # 数据插入self.__sorted_data
                self.__sorted_upstream_buffer_queue.insert(pos, self.upstream_buffer_queue[k])   # 把接收队列插入排序
                self.empty_queue.pop(k)
            except QueueEmpty:
                continue
        # print("结束遍历empty_queue")

        # 从self.__sorted_data取出第一个数据
        # 同时把该队列加入self.empty_queue
        # print(self.__sorted_upstream_buffer_queue)
        # print(self.empty_queue)
        if len(self.__sorted_upstream_buffer_queue) > 0:
            self.empty_queue[self.__sorted_name.pop(0)] = self.__sorted_upstream_buffer_queue.pop(0)
            self.__sorted_time.pop(0)
            return self.__sorted_data.pop(0)
        else:
            raise QueueEmpty

    @asyncio.coroutine
    def async_next(self):
        """
        Using asyncio.Queue, asynchronously yield data from self.__data_source, put it into Queue
        :return:
        """
        print("{} Morphe async next".format(self.name))
        while True:
            try:
                data = yield from self.__next()
                for atomos in self.listener:
                    # TODO: 这里有一个隐患，当个别 atomos 的maxsize特别小，此线程消费速度又不够快的时候，会阻塞其他订阅者
                    yield from atomos.put(data)
                    print("{} async_put: {}".format(self.name, data))
                    # print(atomos)
            except StopIteration:
                print("{} Morphe StopIteration".format(self.name))
            except QueueEmpty:
                print("{} Morphe QueueEmpty".format(self.name))
                a = yield from asyncio.sleep(0.5)

    @asyncio.coroutine
    def async_to_sync(self):
        """
        As long as a sync_queue is set, replace "async_next" with "async_to_sync"
        :return:
        """
        print("{} async_to_sync".format(self.name))
        while True:
            try:
                data = yield from self.__next()
                self.sync_queue.put(data)
            except QueueEmpty:
                print("{} async_to_sync QueueEmpty".format(self.name))
                # TODO: 这里不知道为什么，一旦出现一次QueueEmpty以后，就再也不会往后执行了
                a = yield from asyncio.sleep(0.5)

# coding: utf-8
from threading import Thread
from pandas import DataFrame
import asyncio
import queue
from collections import Iterator
from collections import Iterable
from asyncio import QueueEmpty, QueueFull
import time


def loop_run_forever(lp):
    lp.run_forever()


class Atomos(asyncio.Queue):
    """
    Atomos是希腊文中"不可分割粒子"的意思，拉丁文Atomus
    
    It is a wrapper to make every single data source an asyncio.Queue
    The purpose of Atomos is to provide a producer a "Morphe" to subscribe.
    """

    # TODO: So far, we suppose the single data source is already sorted

    def __init__(
            self,
            data_source=None,
            time_column="time",
            name=None,
            buffer_size=0,
            sync_queue=None,  # 只有当这个Atomos是终端，需要异步转同步队列时，才传入sync_queue
            loop=None
    ):
        if sync_queue is not None:
            self.async_next = self.async_to_sync
            self.sync_queue = sync_queue
            print("{} 异步转同步队列，调用self.async_to_sync".format(name))
        if isinstance(data_source, Atomos):
            loop = data_source._loop
        if loop is None:
            """
            如果event_loop没有传入，就新建一个
            """
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
        if not loop.is_running():
            """
            loop.run_forever()
            """
            import threading
            import functools
            t = threading.Thread(target=functools.partial(loop_run_forever, loop), daemon=True)
            t.start()

        loop.set_debug(enabled=True)
        super().__init__(maxsize=buffer_size, loop=loop)
        asyncio.set_event_loop(self._loop)
        self.time_column = time_column
        self.upstream = dict()  # 上游的数据源列表
        self.listener = list()  # 订阅此源的终端
        if name is None:
            self.name = "{}".format(data_source)
        else:
            self.name = name
        if isinstance(data_source, Atomos):
            print("{}, data_source为Atomos".format(self.name))
            self.__data_source = data_source
            # 直接订阅
            self.subscribe(data_source)
            self.__next = self.get
        elif isinstance(data_source, Iterator):
            print("{}, data_source为Iterator".format(self.name))
            self.__data_source = data_source
            # self.__next不用改变
        elif isinstance(data_source, Iterable):
            print("{}, data_source为Iterable".format(self.name))
            self.__data_source = iter(data_source)  # 将Iterable对象转换成迭代器
            # self.__next不用改变
        elif isinstance(data_source, queue.Queue):
            print("{}, data_source为queue.Queue".format(self.name))
            self.__data_source = data_source
            self.__next = self.async_get_wrapper  # 将同步get封装成coroutine
        elif data_source is None:
            print("{}, data_source为None".format(self.name))
            self.__data_source = None
            self.__next = self.get
        else:
            raise TypeError("data_source is expected to be either an Iterator or Iterable.")

        self.started = False

    def start(self):
        """
        ** NOT thread safe **
        :return: 
        """
        # 将async_next()加入协程池
        # TODO: not thread safe
        # 然后开启上游Atomos
        for name, atomos in self.upstream.items():
            atomos.start()

        # 开启本Atomos
        if not self.started:
            self.started = True
            asyncio.run_coroutine_threadsafe(self.async_next(), self._loop)
            print("Start: {}, {}".format(self.name, self.async_next()))

    def subscribe(self, atomos):
        """
        订阅数据源
        :param atomos 
        :return: 
        """
        atomos.listener.append(self)
        self.upstream[atomos.name] = atomos
        print("{} 订阅了 {}".format(self.name, atomos.name))

    def unsubscribe(self):
        pass

    @asyncio.coroutine
    def async_get_wrapper(self):
        data = self.__data_source.get()
        return data

    @asyncio.coroutine
    def __next(self):
        try:
            data = self.__data_source.__next__()
            return data
        except StopIteration:
            print("{} StopIteration".format(self.name))
            raise StopIteration

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



# class MongoDBCtpMdTick(Atomos):
#     """
#     MongoDB Connector for query single instrument
#     """
#     def __init__(self, db_name="CtpMarketData", collection_name="CtpMd", instrument_id="", start="2017-01-01", end=None):
#         from pymongo import MongoClient
#         from datetime import datetime
#         mongo = MongoClient()
#         db = mongo.get_database(db_name)
#         collection = db.get_collection(collection_name)
#         query = dict()
#         if instrument_id != "":
#             query["InstrumentID"] = instrument_id
#         if start is not None:
#             query["ActionTime"] = dict()
#             query["ActionTime"]["$gt"] = datetime.strptime(start, "%Y-%m-%d")
#         if end is not None:
#             if "ActionTime" not in query:
#                 query["ActionTime"] = dict()
#             query["ActionTime"]["$lt"] = datetime.strptime(start, "%Y-%m-%d")
#
#         cursor = collection.find(query).sort([("ActionTime", 1)])
#         super().__init__(data_source=cursor, time_column="ActionTime")


# class TickToBar(Atomos):
#     """
#     This class accepts a single data source of tick data, convert it to a bar with a certain period.
#
#     In case you need the ask_price or bid_price instead of last_price, simply change the value of "price".
#      We use "LastPrice" by default
#     """
#
#     def __init__(
#             self,
#             data_source=None,
#             time_column="ActionTime",
#             price="LastPrice"
#     ):
#         super().__init__(data_source=data_source, time_column=time_column)
#         self.__tick_buffer = DataFrame(columns=[time_column, price])
#         self.__bar_feed = Queue()
#         clock = Thread(target=self.update_bar, daemon=True)
#
#     def update_bar(self):
#         pass
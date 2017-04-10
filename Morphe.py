# coding: utf-8
import asyncio
from queue import Queue
import random
import time
import bisect


class Atomos:
    """
    Atomos是希腊文中"不可分割粒子"的意思，拉丁文Atomus
    
    The purpose of Atomos is to provide a data source for the class Morphe(defined below, which is a subclass of Atomos)
    
    Atomos should be considered as a single source of data. It takes input of exactly one iterable instance, 
    makes it an iterator with data sorted by timestamp.
    """
    # TODO: So far, we suppose the single data source is already sorted

    def __init__(self, data_source=None):
        from collections import Iterator
        from collections import Iterable
        if isinstance(data_source, Iterator):
            self.__data_source = data_source
        elif isinstance(data_source, Iterable):
            self.__data_source = iter(data_source)
        elif data_source is None:
            self.__data_source = None
        else:
            raise TypeError("data_source is expected to be either Iterator or Iterable.")
        self.buffer = None

    def __iter__(self):
        return self

    def __next__(self):
        """
        The mechanism for us to obtain the next data:

        :return:
            return the next data or raise StopIteration
        """
        return self.__data_source.__next__()

    def _buffer(self):
        try:
            self.buffer = self.__next__()
            return self.buffer
        except StopIteration:
            return None

    @asyncio.coroutine
    def async_next(self):
        try:
            return self.__data_source.__next__()
        except StopIteration:
            return None


class Morphe(Atomos):
    """
    Morphe是希腊文中“形态”的意思，英文词根Morph-来源于此。
    
    The purpose of Morphe is a realtime Data Adapter:
        It takes input of DataFrames, pymongo cursors or any other generators and generates tick data
    
    The accepted data source (which is usually another Morphe type), is supposed to be sorted in the order of "early first"
    
    self.__data_time: initiate as None, indicates the earliest timestamp of data, will change during iteration.
    self.__producer: initiate as a  
    """
    def __init__(self):
        super().__init__()
        self.__data_time = None   # datetime格式，指当前数据中最早的时间
        self.__producer = list()  # 订阅的数据源列表，每个元素都是个Atomos或其子类的实例
        self.__producer_buffer = list()  #
        self.__sorted_producer = list()  # 存放已经排序好的__producer
        self.__sorted_time = list()  # 用于存放已经排序好的时间

    def __iter__(self):
        return self

    def __insert_sort(self):
        if len(self.__producer) > 0:
            morphe = self.__producer[0]
            data = morphe.buffer
            next_data = morphe._buffer()
            self.__sorted_time.pop(0)
            self.__producer.pop(0)
            if next_data is not None:
                i = bisect.bisect_left(self.__sorted_time, next_data["time"])
                self.__sorted_time.insert(i, next_data["time"])
                self.__producer.insert(i, morphe)
            return data
        else:
            raise StopIteration

    def __next__(self):
        """
        return the next data or raise StopIteration
        The mechanism for us to obtain the next data:
         async 
        :return: 
        """
        return self.__insert_sort()

    @asyncio.coroutine
    def get_time(self):
        return time.time()

    def subscribe(self, data_sources, adapter=None):
        """
        订阅数据源
        :param data_sources: 
        :return: 
        """
        if isinstance(data_sources, list):
            for data_source in data_sources:
                if isinstance(data_source, Atomos):
                    data = data_source._buffer()
                    if data is not None:
                        if "time" in data:
                            i = bisect.bisect_left(self.__sorted_time, data["time"])
                            self.__producer.insert(i, data_source)
                            self.__sorted_time.insert(i, data["time"])
                            print("订阅:{}, buffer_time: {}".format(data_sources, data["time"]))
                        else:
                            raise ValueError("要订阅的{}中的数据没有time字段".format(data_source))
                    else:
                        # 没有数据，如果不是实时源就不用订阅了吧
                        # TODO: Solution for real time data source
                        pass
                else:
                    raise TypeError(
                        "Atomos expected, but we got {}".format(type(data_source))
                    )
        elif isinstance(data_sources, Atomos):
            data = data_sources._buffer()
            if data is not None:
                if "time" in data:
                    i = bisect.bisect_left(self.__sorted_time, data["time"])
                    self.__producer.insert(i, data_sources)
                    self.__sorted_time.insert(i, data["time"])
                    print("订阅:{}, buffer_time: {}".format(data_sources, data["time"]))
                else:
                    raise ValueError("要订阅的{}中的数据没有time字段".format(data_sources))
            else:
                # 没有数据，如果不是实时源就不用订阅了吧
                # TODO: Solution for real time data source
                pass
        else:
            raise TypeError(
                "Atomos expected, but we got {}".format(type(data_sources))
            )

    def unsubscribe(self):
        pass


class Adapter:
    def __init__(self, data_source, data_converter):
        self.data_source = data_source

    def __iter__(self):
        pass

    def data_converter(self, data):
        return data

    def __next__(self):
        data = self.data_source.next()
        return self.data_converter(data)

if __name__ == "__main__":
    t = list()
    for i in range(0, 300):
        t.append(time.time())
        time.sleep(0.001)

    # 生成三个数据源
    a1 = Atomos([{"time": t[i]} for i in range(0, 300, 2)])
    a2 = Atomos([{"time": t[i]} for i in range(0, 300, 3)])
    a3 = Atomos([{"time": t[i]} for i in range(0, 300, 5)])

    # 用一个Morphe订阅它们
    m1 = Morphe()
    m1.subscribe(a1)
    m1.subscribe(a2)
    # m1 订阅了a1, a2

    m2 = Morphe()
    m2.subscribe(a3)
    # m2 订阅了a3

    m3 = Morphe()
    m3.subscribe(m1)
    m3.subscribe(m2)
    # 由于subscribe接收的类型是Atomos，Morphe是Atomos的子类，所以
    # m1, m2可以作为一个整体被m3订阅
    # 下面就是m3作为Iterator按"time"顺序输出结果

    count = 0
    for x in m3:
        count += 1
        print(count, x)
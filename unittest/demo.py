# coding: utf-8
from pymongo import MongoClient
from datetime import datetime
from Morphe import Atomos
from Morphe import Morphe
import copy

mongo = MongoClient()
db = mongo.get_database("CtpMarketData")
coll = db.get_collection("CtpMd")

cursor_rb1705 = coll.find(
    {
        "InstrumentID": "rb1705",
        "ActionTime": {"$gt": datetime(2017, 4, 1)}
    }
).sort([("ActionTime", 1)])

cursor_hc1705 = coll.find(
    {
        "InstrumentID": "hc1705",
        "ActionTime": {"$gt": datetime(2017, 4, 1)}
    }
).sort([("ActionTime", 1)])

a1 = Atomos(data_source=cursor_rb1705, time_column="ActionTime")
a2 = Atomos(data_source=cursor_hc1705, time_column="ActionTime")

m = Morphe(time_column="ActionTime")
m.subscribe(copy.deepcopy(a1))
m.subscribe(copy.deepcopy(a2))

n = Morphe(time_column="ActionTime")
n.subscribe(copy.deepcopy(a1))

o = Morphe(time_column="ActionTime")
o.subscribe([m, n])

import time
count = 0
s = time.time()
for item in n:
    count += 1
    print("{}, {}".format(count, item[m.time_column]), item["InstrumentID"])
print(time.time()-s)

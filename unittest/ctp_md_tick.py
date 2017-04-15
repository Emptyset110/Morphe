# coding: utf-8
import sys
from Morphe.Atomos import MongoDBCtpMdTick
import asyncio
sys.path.append("..")


loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
rb1705 = MongoDBCtpMdTick(instrument_id="rb1705", start="2017-04-01")
loop.run_until_complete(asyncio.gather(*[rb1705.async_next()]))

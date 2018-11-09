import asyncio
import json
import re
import time
from decimal import Decimal

import bidict as bidict
from aiohttp import ClientSession
from sqlalchemy import (
    Column, Integer, MetaData, Table, Text, create_engine,
    ForeignKey, BigInteger, select, String, and_)
from sqlalchemy_aio import ASYNCIO_STRATEGY

COLLECTING_INTERVAL = 1
TIME_TO_LIVE = 30 * 60  # 30 min
CLEARING_INTERVAL = 30  # 30 sec
AVAILABLE_ASSETS_MAP = bidict.bidict({
    'EURUSD': 1,
    'USDJPY': 2,
    'GBPUSD': 3,
    'AUDUSD': 4,
    'USDCAD': 5
})

subscribed = {}

myloop = asyncio.get_event_loop()

async_engine = create_engine('sqlite:///test.db', strategy=ASYNCIO_STRATEGY)
# TODO make creating if not exist with async engine
engine = create_engine('sqlite:///test.db')

metadata = MetaData(engine)
assets_table = Table(
    'assets', metadata,
    Column('id', Integer, primary_key=True),
    Column('symbol', Text),
)
ticks_table = Table(
    'ticks', metadata,
    Column('id', Integer, primary_key=True),
    Column('asset_id', Integer, ForeignKey("assets.id")),
    Column('value', String),
    Column('timestamp', BigInteger),
)
metadata.create_all()

# fixtures
with engine.connect() as conn:
    for symbol, asset_id in AVAILABLE_ASSETS_MAP.items():
        result = conn.execute(assets_table.select(assets_table.c.id == asset_id))
        if not result.fetchall():
            conn.execute(assets_table.insert().values(id=asset_id, symbol=symbol))


async def collect_data():
    """
    Collect ticks from ratesjson, parse it and put in the db.
    Additionally, subscribed clients will receive new ticks by asset.
    :return:
    """
    async with ClientSession() as session:
        async with session.get('https://ratesjson.fxcm.com/DataDisplayer') as response:
            raw = await response.read()
    matches = re.findall('{.{10}(?:' + f'{"|".join(AVAILABLE_ASSETS_MAP.keys())}' + ')[^}]+}', raw.decode())
    async with async_engine.connect() as conn:
        for t in matches:
            tick = json.loads(t.replace(',}', '}'))
            ts = round(time.time())
            asset_id = AVAILABLE_ASSETS_MAP[tick['Symbol']]
            result = await conn.execute(
                ticks_table.select().where(and_(
                    ticks_table.c.asset_id == asset_id, ticks_table.c.timestamp == ts)))
            if not await result.fetchall():
                value = str((Decimal(tick['Bid']) + Decimal(tick['Ask'])) / 2)
                await conn.execute(ticks_table.insert().values(
                    asset_id=asset_id,
                    value=value,
                    timestamp=ts
                ))
                data = json.dumps({
                    "action": "point",
                    "message": {
                        "assetName": AVAILABLE_ASSETS_MAP.inv[asset_id],
                        "time": ts,
                        "assetId": asset_id,
                        "value": float(value)
                    }
                }).encode()
                for s_writer, s_asset_id in subscribed.items():
                    if s_asset_id != asset_id:
                        continue
                    myloop.create_task(sending_tick(s_writer, data))


async def sending_tick(writer, data):
    writer.write(data)
    try:
        await writer.drain()
    except:
        del subscribed[writer]


async def clearing():
    """
    Kill old ticks.
    :return:
    """
    earliest_ts = round(time.time()) - TIME_TO_LIVE
    async with async_engine.connect() as conn:
        await conn.execute(ticks_table.delete().where(ticks_table.c.timestamp < earliest_ts))


async def collecting_schedule():
    while True:
        myloop.create_task(collect_data())
        await asyncio.sleep(COLLECTING_INTERVAL)


async def clearing_schedule():
    while True:
        await asyncio.sleep(CLEARING_INTERVAL)
        myloop.create_task(clearing())


async def parse_data(data, writer):
    """
    Parse incoming messages and generate data, depending on message.
    :param data:
    :param writer:
    :return:
    """
    message = data.decode()
    try:
        command = json.loads(message)
        action = command.get('action')
        if action == 'assets':
            command['message'] = {
                "assets": [{"id": v, "name": k} for k, v in AVAILABLE_ASSETS_MAP.items()]
            }
            data = json.dumps(command).encode()
        elif action == 'subscribe':
            asset_id = command['message']['assetId']
            if subscribed.get(writer) != asset_id:
                subscribed[writer] = asset_id
                async with async_engine.connect() as conn:
                    earliest_ts = round(time.time()) - TIME_TO_LIVE
                    j = ticks_table.join(assets_table, and_(
                        ticks_table.c.asset_id == assets_table.c.id,
                        ticks_table.c.timestamp > earliest_ts,
                        ticks_table.c.asset_id == asset_id
                    ))
                    result = await conn.execute(select([
                        assets_table.c.symbol,
                        ticks_table.c.timestamp,
                        ticks_table.c.value
                    ]).select_from(j))
                    fetched = await result.fetchall()
                    ticks = [{
                        "assetName": symbol,
                        "time": timestamp,
                        "assetId": asset_id,
                        "value": float(value)
                    } for symbol, timestamp, value in fetched]
                    data = json.dumps({
                        "action": "asset_history",
                        "message": {"points": ticks}
                    }).encode()
            else:
                data = b"Already subscribed."
    except:
        data = b"Unknown message format."
    return data


async def handle_echo(reader, writer):
    """
    Handle incoming message, send answer to client.
    :param reader:
    :param writer:
    :return:
    """
    while True:
        raw = await reader.read(1024)
        data = await parse_data(raw, writer)
        if data:
            writer.write(data)
            try:
                await writer.drain()
            except ConnectionResetError:
                break
            if writer not in subscribed:
                break
    writer.close()


if __name__ == '__main__':
    myloop.create_task(collecting_schedule())
    myloop.create_task(clearing_schedule())
    myloop.create_task(asyncio.start_server(handle_echo, '', 8080, loop=myloop))
    myloop.run_forever()

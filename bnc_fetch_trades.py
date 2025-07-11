import asyncio
from datetime import datetime

import pandas as pd
import ccxt.async_support as ccxta
from tqdm.asyncio import tqdm
from loguru import logger

TS1 = 1725908400000  # 2024-09-10
TS2 = 1739300400000  # 2025-02-12
TS3 = 1752087600000  # 2025-07-10
TOP_PERPS = pd.read_parquet("data/top_usdt_perps.gzip")

logger.add(
    "logs/debug.log",
    format="[{time}] {level} : {module} : {function} : {line}\n{message}",
    level="DEBUG",
)


async def fetch_2h_perp_trades(
    bnc: ccxta.binance, symbol: str, since: int
) -> pd.DataFrame:
    try:
        until = since + 7_200_000
        trades = await bnc.fetch_trades(
            symbol,
            since=since,
            limit=1000,
            params={"fetchTradesMethod": "fapiPublicGetAggTrades"},
        )
        last_trade_ts = trades[-1]["timestamp"]
        last_trade_id = trades[-1]["id"]
        while last_trade_ts < until:
            next_trades = await bnc.fetch_trades(
                symbol,
                limit=1000,
                params={
                    "fetchTradesMethod": "fapiPublicGetAggTrades",
                    "fromId": last_trade_id,
                },
            )
            if len(next_trades) <= 1:
                break
            trades.extend(next_trades[1:])
            last_trade_ts = next_trades[-1]["timestamp"]
            last_trade_id = next_trades[-1]["id"]
        trades = pd.DataFrame(trades)[
            ["timestamp", "symbol", "id", "side", "price", "amount", "cost"]
        ].query("timestamp < @until")
    except Exception as e:
        logger.exception(f"Error fetching trades for {symbol}: {e}")
        raise e
    finally:
        await bnc.close()
    return trades

async def fetch_all_24h_trades(start: int) -> pd.DataFrame:
    async with ccxta.binance() as bnc:
        for since in range(start, start + 86_400_000, 7_200_000):
            tasks = [fetch_2h_perp_trades(bnc, symbol, since) for symbol in TOP_PERPS["symbol"]]
            trades_data = await tqdm.gather(*tasks[:30])
            trades_delta = await tqdm.gather(*tasks[30:60])
            trades_data.extend(trades_delta)
            trades_delta = await tqdm.gather(*tasks[60:])
            trades_data.extend(trades_delta)
            trades_data = pd.concat(trades_data)
            since_dt = datetime.fromtimestamp(since / 1000).strftime("%Y-%m-%d_%H")
            trades_data.to_parquet(f"data/trades_{since_dt}.gzip", compression="gzip", index=False)


if __name__ == "__main__":
    for start in [TS1, TS2, TS3]:
        asyncio.run(fetch_all_24h_trades(start))
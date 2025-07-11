import asyncio
from datetime import datetime

import pandas as pd
import ccxt.async_support as ccxta
from tqdm.asyncio import tqdm

TS1 = 1725908400000  # 2024-09-10
TS2 = 1739300400000  # 2025-02-12
TS3 = 1752087600000  # 2025-07-10

TOP_PERPS = pd.read_parquet("data/top_usdt_perps.gzip")


async def fetch_4h_perp_trades(
    bnc: ccxta.binance, symbol: str, since: int
) -> pd.DataFrame:
    until = since + 14_400_000
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
    return trades

async def fetch_all_24h_trades(start: int) -> pd.DataFrame:
    async with ccxta.binance() as bnc:
        tasks = []
        for since in range(start, start + 86_400_000, 14_400_000):
            tasks.extend([fetch_4h_perp_trades(bnc, symbol, since) for symbol in TOP_PERPS["symbol"]])
        trades = await tqdm.gather(*tasks)
        trades = pd.concat(trades)
    return trades



if __name__ == "__main__":
    for start in [TS1, TS2, TS3]:
        trades = asyncio.run(fetch_all_24h_trades(start))
        start_date = datetime.fromtimestamp(start / 1000).strftime("%Y-%m-%d")
        trades.to_parquet(f"data/trades_{start_date}.gzip", compression="gzip", index=False)
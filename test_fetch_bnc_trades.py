import asyncio
from datetime import datetime

import pandas as pd
import ccxt.async_support as ccxta
from tqdm.asyncio import tqdm

TS3 = 1752087600000
TOP_PERPS = pd.read_parquet("data/top_usdt_perps.gzip")["symbol"]

async def fetch_1h_perp_trades(
    bnc: ccxta.binance, symbol: str, since: int
) -> pd.DataFrame:
    until = since + 3_600_000
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

async def main():
    async with ccxta.binance() as bnc:
        trades = await fetch_1h_perp_trades(bnc, "BCH/USDT", TS3)
        trades.to_parquet("data/trades_test1.gzip", compression="gzip", index=False)
        print(f"last trade: {datetime.fromtimestamp(trades['timestamp'].max() / 1000)}")

if __name__ == "__main__":
    asyncio.run(main())
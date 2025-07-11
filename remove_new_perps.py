import pandas as pd

df = pd.read_parquet("data/top_usdt_perps.gzip")
remove_list = [
    "KAITO/USDT:USDT",
    "FORM/USDT:USDT",
    "JST/USDT:USDT",
    "SYRUP/USDT:USDT",
    "A/USDT:USDT",
    "TRUMP/USDT:USDT",
    "POL/USDT:USDT",
    "EIGEN/USDT:USDT",
    "KAIA/USDT:USDT",
    "MOVE/USDT:USDT",
    "RAYSOL/USDT:USDT",
    "VIRTUAL/USDT:USDT",
    "PENGU/USDT:USDT",
    "KMNO/USDT:USDT",
    "DEXE/USDT:USDT",
    "S/USDT:USDT",
]

df.info()

df = df[~df["symbol"].isin(remove_list)]

df.to_parquet("data/top_usdt_perps.gzip", compression="gzip", index=False)

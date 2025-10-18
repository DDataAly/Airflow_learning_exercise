import pandas as pd
import os
from datetime import datetime
from src.exchanges.binance import BinanceExchange
from src.exchanges.kraken import KrakenExchange
from src.exchanges.coinbase import CoinbaseExchange


def set_up_exchanges(exchanges_list = None):
    exchange_objects = []
    for name in exchanges_list:
        if name.lower() == 'kraken':
            exchange_objects.append(KrakenExchange(
                                                "kraken",
                                                "https://api.kraken.com/0/public/Depth",
                                                {"pair": "XXBTZUSD", "count": 20},
                                                "XXBTZUSD",
                                    ))
        if name.lower() =='binance':   
            exchange_objects.append(BinanceExchange(
                                                "binance",
                                                "https://api.binance.com/api/v3/depth",
                                                {"symbol": "BTCUSDT", "limit": 20},
                                                "BTCUSDT",
                                            )) 
        if name.lower() == 'coinbase':
            exchange_objects.append(CoinbaseExchange(
                                                "coinbase",
                                                "https://api.exchange.coinbase.com/products/BTC-USD/book",
                                                {"level": 2},
                                                "BTC-USD",
                                            ))
        return exchange_objects

def get_request_time() -> str:
    request_time = datetime.now()
    return request_time.strftime("%Y-%m-%d_%H:%M:%S")


def create_best_price_df(best_price: list) -> pd.DataFrame:
    best_price_dict = {
        "price": round(float(best_price[0]), 2),
        "exchange": best_price[1],
        "request_time": best_price[2],
    }
    best_price_df = pd.DataFrame([best_price_dict])
    return best_price_df


def record_best_price(best_price_df: pd.DataFrame, path: str):
    file_name = f"{path}/best_deal.csv"
    col_headers = False if os.path.isfile(file_name) else True
    best_price_df.to_csv(file_name, mode="a", header=col_headers, index=False)

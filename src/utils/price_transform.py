import logging
import os
import json
from datetime import datetime


def new_find_best_price(exchanges: list, request_time: datetime , path:str, vol_per_order: float) -> list:
    prices = []
    for exchange in exchanges:
        try:
            dir_path = os.path.join(path, exchange.name)
            file_to_read = os.path.join(dir_path, f"{exchange.name}_{str(request_time)}.json")
            with open (file_to_read, 'r') as file:
                snapshot = json.load(file)
            
            price_volume_df = exchange.parse_snapshot_to_df(snapshot)
            exchange.order_cost_total = exchange.calculate_order_price(
                price_volume_df, vol_per_order
            )
            prices.append((float(exchange.order_cost_total), exchange.name))

        except TypeError:
            logging.error(f"Type error when parsing {exchange.name} snapshot.")
        except Exception as e:
            logging.error(f"{exchange.name} An error has occurred: {e}")

    if prices:
        prices.sort()
        best_price = list(prices[0])
        best_price.append(request_time)
        return best_price
    else:
        logging.warning(
            f"No exchanges could provide a valid price at {request_time}."
            " Increase the order book depth or split order on more transactions"
        )
        return ["Not available", "Checked all exchanges", request_time]

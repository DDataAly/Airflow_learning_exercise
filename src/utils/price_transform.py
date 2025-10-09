import logging
from datetime import datetime


def choose_best_price(exchanges: list, fetched_data:list, vol_per_order: float) -> list:
    all_quotes = []
    for record in fetched_data:
        if record is None:
            continue

        exchange_name, request_time, snapshot = record['exchange'], record['request_time'], record['snapshot'] 

        try:
            exchange = next(e for e in exchanges if e.name == exchange_name)
            price_volume_df = exchange.parse_snapshot_to_df(snapshot)
            exchange.order_cost_total = exchange.calculate_order_price(
                price_volume_df, vol_per_order
            )
            all_quotes.append({'exchange':exchange_name,
                               'request_time':request_time,
                               'order_cost': float(exchange.order_cost_total)})
                          
        except Exception as e:
            logging.error(f"{exchange.name} An error has occurred at the transformation stage: {e}")

    if not all_quotes:
        logging.warning('None of the exchange snapshots has enough depth to fulfill this order. Check the requested depth in the set_up_exchanges() function')
        return None
    
    if all_quotes:
        all_quotes.sort(key = lambda x: x['order_cost'])
        best_quote = all_quotes[0]

        logging.info (f'Transform stage is completed. This is the best quote: {best_quote}')
        logging.info (f'All quotes: {all_quotes}')

        return {'best_quote': best_quote, 'all_quotes': all_quotes}
    

# Can delete everything after this line
# Full working version: import logging
# import os
# import json
# from datetime import datetime


# def choose_best_price(exchanges: list, request_time: datetime , path:str, vol_per_order: float) -> list:
#     prices = []
#     for exchange in exchanges:
#         try:
#             dir_path = os.path.join(path, exchange.name)
#             file_to_read = os.path.join(dir_path, f"{exchange.name}_{str(request_time)}.json")
#             with open (file_to_read, 'r') as file:
#                 snapshot = json.load(file)
            
#             price_volume_df = exchange.parse_snapshot_to_df(snapshot)
#             exchange.order_cost_total = exchange.calculate_order_price(
#                 price_volume_df, vol_per_order
#             )
#             prices.append((float(exchange.order_cost_total), exchange.name))

#         except FileNotFoundError:
#             logging.error(f'Can\'t find the file for {exchange.name}: {file_to_read}')
#         except TypeError:
#             logging.error(f"Type error when parsing {exchange.name} snapshot.")
#         except Exception as e:
#             logging.error(f"{exchange.name} An error has occurred: {e}")

#     if prices:
#         prices.sort()
#         best_price = list(prices[0])
#         best_price.append(request_time)
#         return best_price
#     else:
#         logging.warning(
#             f"No exchanges could provide a valid price at {request_time}."
#             " Increase the order book depth or split order on more transactions"
#         )
#         return ["Not available", "Checked all exchanges", request_time]
# # 
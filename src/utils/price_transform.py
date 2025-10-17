import logging

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
        best_quote = min(all_quotes, key=lambda x: x['order_cost'])

        logging.info (f'Transform stage is completed. This is the best quote: {best_quote}')
        logging.info (f'All quotes: {all_quotes}')

        return {'best_quote': best_quote, 'all_quotes': all_quotes}
    


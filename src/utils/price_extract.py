import logging
from datetime import datetime

def get_price(exchanges: list, path: str, request_time: datetime ) -> list:
    for exchange in exchanges:
        try:
            snapshot = exchange.request_order_book_snapshot()
            # if snapshot is None or exchange.validate_snapshot(snapshot) is False:
            if exchange.validate_snapshot(snapshot) is False:
                logging.warning(
                    f"{exchange.name}: invalid or missing snapshot at {request_time}"
                )
                continue

            snapshot = exchange.trim_snapshot(snapshot)
            exchange.save_order_book_snapshot(snapshot, path, request_time)

        except Exception as e:
            logging.error(f"{exchange.name} An error has occurred at the extract stage: {e}")




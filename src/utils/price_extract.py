import logging
from datetime import datetime

def get_price(exchange, request_time: datetime ) -> str:
    try:
            snapshot = exchange.request_order_book_snapshot()
            if not exchange.validate_snapshot(snapshot):
                logging.warning(f"{exchange.name}: invalid snapshot at {request_time}")
                return None

            trimmed = exchange.trim_snapshot(snapshot)
            return {
                "exchange": exchange.name,
                "request_time": request_time,
                "snapshot": trimmed,
            }
    
    except Exception as e:
        logging.error(f"{exchange.name} An error has occurred at the extract stage: {e}")
        return None
    

import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def load_quotes_to_db(transform_output, vol_per_order: float):
    if transform_output is None:
        logging.warning("No data to load.")
        return None

    # Load environmental variables
    load_dotenv()
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    database = os.environ["DB_NAME"]
    engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/{database}")

    best = transform_output["best_quote"]
    all_quotes = transform_output["all_quotes"]

    with engine.begin() as conn:
        # Insert best price, return run_id
        # this will return run-id as a sqlalchemy.engine.Result object, not an integer, which we need to convert to an integer 
        # sqlalchemy.engine.Result has a method scalar_one() which returns exactly one scalar result (i.e. one value) or raises an exception
        result = conn.execute(
            text("""
                INSERT INTO best_price_quotes (order_volume, order_cost_usd)
                VALUES (:volume, :cost)
                RETURNING run_id;  
            """),
            {"volume": vol_per_order, "cost": best["order_cost"]},
        )
        run_id = result.scalar_one()

        for quote in all_quotes:
            # Insert exchange if not exists - on conflict clause prevents creating duplicates of existing exchanges on each run
            conn.execute(
                text("""
                    INSERT INTO exchanges (name)
                    VALUES (:name)
                    ON CONFLICT (name) DO NOTHING;
                """),
                {"name": quote["exchange"]},
            )
            # Returns the exchange_id, which is a primary key of the exchanges table as int.
            # We need it as it's foreign key in the exchanges_quotes_snapshots table
            # We can't return it within the previous statement because of the ON_CONFLICT (name) DO NOTHING clause
            exchange_id = conn.execute(
                text("SELECT exchange_id FROM exchanges WHERE name = :name"),
                {"name": quote["exchange"]},
            ).scalar_one()

            # Insert detailed snapshot
            conn.execute(
                text("""
                    INSERT INTO exchange_quotes_snapshots
                        (run_id, exchange_id, snapshot_time, quote_volume, quote_cost)
                    VALUES (:run_id, :exchange_id, :time, :volume, :cost);
                """),
                {
                    "run_id": run_id,
                    "exchange_id": exchange_id,
                    "time": quote["request_time"],
                    "volume": vol_per_order,
                    "cost": quote["order_cost"],
                },
            )

    logging.info(f"Data loaded successfully for run_id={run_id}")
    return run_id
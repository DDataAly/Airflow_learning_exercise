from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

def set_up_db():
    load_dotenv()
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASSWORD']
    database = os.environ['DB_NAME']

    engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/{database}", echo=True)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_dir, "schema.sql")

    with engine.begin() as conn:  # begin() gives you a transaction
        with open(schema_path, "r") as f:
            schema = f.read()

        # split by semicolon to support multiple CREATE TABLE statements
        for statement in schema.split(";"):
            if statement.strip():
                conn.execute(text(statement))

    print(f'Database has been successfully initialised')

if __name__ =='__main__':
    set_up_db()


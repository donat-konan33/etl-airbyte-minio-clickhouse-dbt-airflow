import os
from sqlalchemy import create_engine, text
DB_URL = os.environ.get("CLICKHOUSE_DATABASE_URL")
engine = create_engine(DB_URL)

if __name__ == "__main__":
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT * FROM information_schema.tables
                WHERE table_schema = 'datawarehouse'
            """)
        )
        rows = result.mappings().all()
        tables = [table for table in rows]
        print(tables)

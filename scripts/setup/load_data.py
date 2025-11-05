import psycopg2
import pandas as pd
from io import StringIO
from typing import Dict, List
import time

class DataLoader:
    def __init__(self, connection_string: str):
        self.conn = psycopg2.connect(connection_string)
        self.cursor = self.conn.cursor()
    
    def create_schema_adult_census(self, table_name: str):
        """Create schema for adult census dataset"""
        # self.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        # self.conn.commit()
        schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            age INTEGER,
            workclass TEXT,
            fnlwgt INTEGER,
            education TEXT,
            education_num INTEGER,
            marital_status TEXT,
            occupation TEXT,
            relationship TEXT,
            race TEXT,
            sex TEXT,
            capital_gain INTEGER,
            capital_loss INTEGER,
            hours_per_week INTEGER,
            native_country TEXT,
            income TEXT
        );
        """
        self.cursor.execute(schema)
        self.conn.commit()

    def create_schema_healthcare_census(self, table_name: str):
        """Create schema for adult census dataset"""
        # self.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        # self.conn.commit()
        schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT,
            age INT,
            gender TEXT,
            blood_type TEXT,
            medical_condition TEXT,
            date_of_admission DATE,
            doctor TEXT,
            hospital TEXT,
            insurance_provider TEXT,
            billing_amount NUMERIC(10,2),
            room_number TEXT,
            admission_type TEXT,
            discharge_date DATE,
            medication TEXT,
            test_results TEXT
        );
        """
        
        self.cursor.execute(schema)
        self.conn.commit()
    
    def load_csv_copy(self, csv_path: str, table_name: str) -> float:
        """Fast load using COPY command"""
        
        start = time.time()

        with open(csv_path, 'r', encoding='utf-8') as f:
            # Skip header explicitly
            next(f)
            self.cursor.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV",
                f
            )

        self.conn.commit()
        return time.time() - start
    
    def truncate_table(self, table_name: str):
        """Remove all data from table"""
        self.cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        self.conn.commit()
    
    def create_indexes(self, table_name: str, index_config: Dict):
        """Create indexes based on configuration"""
        for idx_name, idx_def in index_config.items():
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} {idx_def};"
            self.cursor.execute(sql)
        self.conn.commit()
    
    def close(self):
        self.cursor.close()
        self.conn.close()

# Example usage
if __name__ == '__main__':
    loader = DataLoader("postgresql://postgres:postgres@localhost:5432/benchmark")
    
    # Create and load tables
    # for size in [100_000, 1_000_000, 5_000_000]:
    #     table_name = f"adult_raw_{size}"
    #     loader.create_schema_adult_census(table_name)
    #     loader.truncate_table(table_name)
    #     load_time = loader.load_csv_copy(f'data/processed/adult_{size}.csv', table_name)
    #     print(f"Loaded {table_name} in {load_time:.2f}s")

    for size in [100_000, 1_000_000, 5_000_000]:
        table_name = f"healthcare_raw_{size}"
        loader.create_schema_healthcare_census(table_name)
        loader.truncate_table(table_name)
        load_time = loader.load_csv_copy(f'data/processed/healthcare_{size}.csv', table_name)
        print(f"Loaded {table_name} in {load_time:.2f}s")
    
    loader.close()
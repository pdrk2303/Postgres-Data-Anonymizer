import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@localhost:5432/benchmark")

df = pd.read_csv("data/raw/adult_census.csv")
df.insert(0, 'id', range(1, len(df) + 1))
df.to_csv("data/processed/adult_census.csv", index=False)
df.to_sql("adult_raw", engine, if_exists="replace", index=False)
print("Ingested adult.csv into adult_raw table.")

df2 = pd.read_csv("data/raw/healthcare_dataset.csv")
df2.insert(0, 'id', range(1, len(df2) + 1))
df2.to_csv("data/processed/healthcare_dataset.csv", index=False)
df2.to_sql("healthcare_raw", engine, if_exists="replace", index=False)
print("Ingested healthcare_dataset.csv into health_raw table.")
import os
import pandas as pd
from pathlib import Path
import requests
from tqdm import tqdm

def download_adult_census():
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
    columns = ['age', 'workclass', 'fnlwgt', 'education', 'education_num',
               'marital_status', 'occupation', 'relationship', 'race', 'sex',
               'capital_gain', 'capital_loss', 'hours_per_week', 'native_country', 'income']
    
    df = pd.read_csv(url, names=columns, skipinitialspace=True)
    df.insert(0, 'id', range(1, len(df) + 1))
    df.to_csv('data/raw/adult_census.csv', index=False)
    return df

def download_healthcare():
    print("Download healthcare dataset from:")
    print("https://www.kaggle.com/datasets/prasad22/healthcare-dataset")
    print("Place in data/raw/healthcare.csv")

def generate_high_cardinality_synthetic(n_rows=100000):
    from faker import Faker
    import uuid
    from datetime import datetime, timedelta
    
    fake = Faker()
    Faker.seed(42)
    
    data = {
        'uuid': [str(uuid.uuid4()) for _ in range(n_rows)],
        'timestamp': [fake.date_time_between(start_date='-2y', end_date='now') 
                      for _ in range(n_rows)],
        'email': [fake.email() for _ in range(n_rows)],
        'ip_address': [fake.ipv4() for _ in range(n_rows)],
        'user_agent': [fake.user_agent() for _ in range(n_rows)],
        'session_id': [fake.sha256() for _ in range(n_rows)],
        'value': [fake.random_int(min=1, max=10000) for _ in range(n_rows)]
    }
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw/high_cardinality.csv', index=False)
    return df

if __name__ == '__main__':
    os.makedirs('data/raw', exist_ok=True)
    download_adult_census()
    download_healthcare()
    # generate_high_cardinality_synthetic()
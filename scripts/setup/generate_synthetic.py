import pandas as pd
import numpy as np
from faker import Faker
from typing import Dict, Any
import yaml

class SyntheticDataGenerator:
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
    
    def scale_adult_census(self, base_df: pd.DataFrame, target_rows: int) -> pd.DataFrame:
        """Scale adult census maintaining distributions"""
        if target_rows <= len(base_df):
            sampled = base_df.sample(n=target_rows, random_state=42).reset_index(drop=True)
            # Reassign IDs to be sequential
            sampled['id'] = range(1, target_rows + 1)
            return sampled
        
        # Calculate scaling factor
        scale_factor = target_rows / len(base_df)
        
        # Replicate and add noise
        n_copies = int(np.ceil(scale_factor))
        scaled_df = pd.concat([base_df] * n_copies, ignore_index=True)
        
        # Add synthetic variation
        scaled_df = self._add_noise_to_dataframe(scaled_df, base_df)
        
        # Sample to exact size
        scaled_df = scaled_df.sample(n=target_rows, random_state=42).reset_index(drop=True)
        
        # CRITICAL FIX: Reassign unique sequential IDs
        scaled_df['id'] = range(1, target_rows + 1)
        
        return scaled_df
    
    def _add_noise_to_dataframe(self, df: pd.DataFrame, base_df: pd.DataFrame) -> pd.DataFrame:
        """Add realistic noise while preserving distributions"""
        df = df.copy()
        
        # Numeric columns: add Gaussian noise (EXCEPT 'id')
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        numeric_cols = [col for col in numeric_cols if col != 'id']  # Exclude 'id'
        
        for col in numeric_cols:
            noise_scale = base_df[col].std() * 0.05  # 5% noise
            df[col] = df[col] + np.random.normal(0, noise_scale, len(df))
            df[col] = df[col].clip(base_df[col].min(), base_df[col].max())

            if pd.api.types.is_integer_dtype(base_df[col]):
                df[col] = df[col].round().astype(int)
        
        # Categorical columns: occasional random replacement
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            unique_vals = base_df[col].unique()
            mask = np.random.random(len(df)) < 0.05  # Replace 5%
            df.loc[mask, col] = np.random.choice(unique_vals, mask.sum())
        
        return df
    
    def generate_quasi_identifiers(self, n_rows: int) -> pd.DataFrame:
        """Generate dataset with known quasi-identifiers for re-identification tests"""
        data = {
            'user_id': range(1, n_rows + 1),
            'zip_code': [self.fake.zipcode()[:5] for _ in range(n_rows)],
            'birth_year': np.random.randint(1940, 2005, n_rows),
            'gender': np.random.choice(['M', 'F', 'O'], n_rows, p=[0.49, 0.49, 0.02]),
            'diagnosis_code': [f'D{np.random.randint(100, 999)}' for _ in range(n_rows)],
            'visit_date': [self.fake.date_between(start_date='-2y', end_date='today') 
                          for _ in range(n_rows)],
            'sensitive_value': np.random.uniform(100, 10000, n_rows)
        }
        return pd.DataFrame(data)

if __name__ == '__main__':
    generator = SyntheticDataGenerator()
    
    # Scale adult census
    base_adult = pd.read_csv('data/raw/adult_census.csv')
    for size in [100_000, 1_000_000]:
        scaled = generator.scale_adult_census(base_adult, size)
        scaled.to_csv(f'data/processed/adult_{size}.csv', index=False)
        print(f"Generated adult_{size}.csv with {len(scaled)} rows and {scaled['id'].nunique()} unique IDs")

    base_healthcare = pd.read_csv('data/raw/healthcare_dataset.csv')
    for size in [100_000, 1_000_000]:
        scaled = generator.scale_adult_census(base_healthcare, size)
        scaled.to_csv(f'data/processed/healthcare_{size}.csv', index=False)
        print(f"Generated healthcare_{size}.csv with {len(scaled)} rows and {scaled['id'].nunique()} unique IDs")
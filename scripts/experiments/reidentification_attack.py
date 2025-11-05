#!/usr/bin/env python3
"""
Simulate re-identification attack using quasi-identifiers
Tests how well masking protects against linkage attacks

Scenario: Attacker has external data with (age, education, sex, race)
          Tries to link to masked database records

Measures: Success rate of correct linkage for different masking methods
"""

import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path

np.random.seed(42)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'benchmark',
    'user': 'postgres',
    'password': 'postgres'
}

def create_external_dataset(conn, sample_size=1000):
    """
    Simulate external dataset attacker might have
    Sample random records with quasi-identifiers
    """
    print(f">>> Creating simulated external dataset ({sample_size} records)...")
    
    query = f"""
        SELECT 
            id,
            age,
            education,
            sex,
            race,
            occupation,
            income
        FROM adult_raw_100000
        ORDER BY random()
        LIMIT {sample_size}
    """
    
    df_external = pd.read_sql(query, conn)
    print(f"  ✓ Created external dataset with {len(df_external)} records")
    
    return df_external

def attempt_linkage(conn, external_df, target_table, quasi_identifiers):
    """
    Attempt to link external records to target table using quasi-identifiers
    
    Returns:
        - match_rate: % of records correctly linked
        - unique_match_rate: % of records with unique match
        - false_match_rate: % of records incorrectly linked
    """
    print(f"\n>>> Attempting linkage on: {target_table}")
    print(f"    Using quasi-identifiers: {quasi_identifiers}")
    
    # Build WHERE clause for matching
    matches = []
    unique_matches = []
    false_matches = []
    
    for idx, external_row in external_df.iterrows():
        # Build matching query
        conditions = []
        params = []
        
        for qi in quasi_identifiers:
            value = external_row[qi]
            
            # Handle NULL values
            if pd.isna(value):
                conditions.append(f"{qi} IS NULL")
            else:
                conditions.append(f"{qi} = %s")
                params.append(value)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT id, {', '.join(quasi_identifiers)}
            FROM {target_table}
            WHERE {where_clause}
        """
        
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            matching_rows = cur.fetchall()
            cur.close()
            
            if len(matching_rows) == 0:
                # No match found
                continue
            elif len(matching_rows) == 1:
                # Unique match
                matched_id = matching_rows[0][0]
                
                if matched_id == external_row['id']:
                    matches.append(external_row['id'])
                    unique_matches.append(external_row['id'])
                else:
                    false_matches.append(external_row['id'])
            else:
                # Multiple matches (ambiguous)
                matched_ids = [row[0] for row in matching_rows]
                
                if external_row['id'] in matched_ids:
                    matches.append(external_row['id'])
        
        except Exception as e:
            # Query might fail for masked columns (e.g., hashed values don't match)
            continue
    
    total = len(external_df)
    match_rate = len(matches) / total
    unique_match_rate = len(unique_matches) / total
    false_match_rate = len(false_matches) / total
    
    print(f"    Results:")
    print(f"      - Total matches: {len(matches)}/{total} ({match_rate*100:.1f}%)")
    print(f"      - Unique correct: {len(unique_matches)}/{total} ({unique_match_rate*100:.1f}%)")
    print(f"      - False matches: {len(false_matches)}/{total} ({false_match_rate*100:.1f}%)")
    
    return {
        'table': target_table,
        'match_rate': match_rate,
        'unique_match_rate': unique_match_rate,
        'false_match_rate': false_match_rate,
        'total_records': total,
        'matched': len(matches),
        'unique_matched': len(unique_matches),
        'false_matched': len(false_matches)
    }

def run_reidentification_experiments():
    """
    Run re-identification attacks against different masked tables
    """
    print("=== Re-identification Attack Simulation ===\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Create external dataset (attacker's auxiliary data)
    external_df = create_external_dataset(conn, sample_size=1000)
    
    # Define quasi-identifiers (attributes that could identify individuals)
    quasi_identifiers = ['age', 'education', 'sex', 'race']
    
    # Tables to attack
    target_tables = {
        'raw': 'adult_raw_100000',
        'view_masked': 'adult_masked_view',
        'static_masked': 'adult_static_masked',
        'hash': 'adult_mask_hash',
        'partial': 'adult_mask_partial',
        'shuffle': 'adult_mask_shuffle',
        'generalize': 'adult_mask_generalize'
    }
    
    results = []
    
    for name, table in target_tables.items():
        try:
            result = attempt_linkage(conn, external_df, table, quasi_identifiers)
            result['masking_method'] = name
            results.append(result)
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results.append({
                'masking_method': name,
                'table': table,
                'match_rate': 0,
                'unique_match_rate': 0,
                'false_match_rate': 0,
                'error': str(e)
            })
    
    conn.close()
    
    # Save results
    output_dir = Path('results/raw')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_dir / 'reidentification_results.csv', index=False)
    df_results.to_json(output_dir / 'reidentification_results.json', orient='records', indent=2)
    
    # Print summary
    print("\n" + "="*70)
    print("Re-identification Success Rates")
    print("="*70)
    print(f"{'Method':<20} {'Match Rate':<15} {'Unique Correct':<15} {'False Positive':<15}")
    print("-"*70)
    
    for _, row in df_results.iterrows():
        print(f"{row['masking_method']:<20} "
              f"{row['match_rate']*100:<15.1f}% "
              f"{row['unique_match_rate']*100:<15.1f}% "
              f"{row['false_match_rate']*100:<15.1f}%")
    
    print("="*70)
    print("\n✓ Results saved to results/raw/reidentification_results.csv")
    
    # Interpretation
    print("\n>>> Interpretation:")
    print("  - High match rate = Poor privacy protection")
    print("  - Low match rate = Better privacy protection")
    print("  - Shuffle/Generalize should have lower rates than raw/partial")

if __name__ == '__main__':
    run_reidentification_experiments()
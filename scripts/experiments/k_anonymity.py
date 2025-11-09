#!/usr/bin/env python3
"""
Implement k-anonymity through generalization and suppression
Creates preprocessed tables satisfying k={2,5,10,20}
Measures utility loss: MAE, relative error, and query latency

Why: postgresql_anonymizer doesn't provide k-anonymity natively.
     This demonstrates the manual effort required and measures utility loss.
"""

import psycopg2
import pandas as pd
import argparse
import time
import statistics
from collections import Counter
from pathlib import Path
import json

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'benchmark',
    'user': 'postgres',
    'password': 'postgres'
}

def generalize_age(age, granularity=5):
    """Generalize age to buckets"""
    return (age // granularity) * granularity

def generalize_education(education):
    """Map education to broader categories"""
    mapping = {
        'Preschool': 'Primary',
        '1st-4th': 'Primary',
        '5th-6th': 'Primary',
        '7th-8th': 'Middle',
        '9th': 'High-School',
        '10th': 'High-School',
        '11th': 'High-School',
        '12th': 'High-School',
        'HS-grad': 'High-School',
        'Some-college': 'College',
        'Assoc-voc': 'College',
        'Assoc-acdm': 'College',
        'Bachelors': 'College',
        'Masters': 'Graduate',
        'Prof-school': 'Graduate',
        'Doctorate': 'Graduate'
    }
    return mapping.get(education.strip(), 'Other')

def check_k_anonymity(df, quasi_identifiers, k):
    """
    Check if dataframe satisfies k-anonymity
    Returns: (satisfies_k, min_group_size, violating_groups)
    """
    grouped = df.groupby(quasi_identifiers).size()
    min_size = grouped.min()
    violations = grouped[grouped < k]
    
    return min_size >= k, min_size, len(violations)

def suppress_small_groups(df, quasi_identifiers, k):
    """Remove rows that belong to groups smaller than k"""
    group_sizes = df.groupby(quasi_identifiers).size()
    valid_groups = group_sizes[group_sizes >= k].index
    
    # Create boolean mask for valid rows
    mask = df.set_index(quasi_identifiers).index.isin(valid_groups)
    suppressed_df = df[mask].reset_index(drop=True)
    
    return suppressed_df, len(df) - len(suppressed_df)

def measure_query_latency(conn, query, runs=5):
    """Measure query execution time"""
    cur = conn.cursor()
    
    # Warmup
    for _ in range(2):
        cur.execute(query)
        cur.fetchall()
    
    # Measure
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        cur.execute(query)
        result = cur.fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    cur.close()
    return statistics.median(times), result

def compute_aggregate_metrics(conn, original_table, kanon_table):
    """
    Compare aggregate query results between original and k-anonymous tables
    Returns: MAE, relative error, latency overhead
    """
    
    # Test queries for accuracy measurement
    queries = {
        'count_by_education': """
            SELECT education_generalized, COUNT(*) as cnt
            FROM {table}
            GROUP BY education_generalized
            ORDER BY education_generalized
        """,
        'avg_age_by_sex': """
            SELECT sex, AVG(age_generalized) as avg_age
            FROM {table}
            GROUP BY sex
            ORDER BY sex
        """,
        'count_by_income': """
            SELECT income, COUNT(*) as cnt
            FROM {table}
            GROUP BY income
            ORDER BY income
        """
    }
    
    results = {}
    
    for query_name, query_template in queries.items():
        # Query original table
        original_query = query_template.format(table=original_table)
        orig_latency, orig_result = measure_query_latency(conn, original_query)
        
        # Query k-anonymous table
        kanon_query = query_template.format(table=kanon_table)
        kanon_latency, kanon_result = measure_query_latency(conn, kanon_query)
        
        # Compute MAE
        if orig_result and kanon_result:
            # Convert to dicts for comparison
            orig_dict = {row[0]: float(row[1]) for row in orig_result}
            kanon_dict = {row[0]: float(row[1]) for row in kanon_result}
            
            # Compute absolute errors
            errors = []
            for key in orig_dict.keys():
                if key in kanon_dict:
                    error = abs(orig_dict[key] - kanon_dict[key])
                    errors.append(error)
                    
                    # Also compute relative error
                    if orig_dict[key] > 0:
                        rel_error = error / orig_dict[key]
                    else:
                        rel_error = 0
            
            mae = sum(errors) / len(errors) if errors else 0
            avg_rel_error = sum([abs(orig_dict[k] - kanon_dict.get(k, 0)) / orig_dict[k] 
                                 for k in orig_dict.keys() if orig_dict[k] > 0]) / len(orig_dict) if orig_dict else 0
            
            results[query_name] = {
                'mae': mae,
                'relative_error': avg_rel_error,
                'orig_latency_ms': orig_latency,
                'kanon_latency_ms': kanon_latency,
                'latency_overhead_pct': ((kanon_latency - orig_latency) / orig_latency * 100) if orig_latency > 0 else 0
            }
    
    # Aggregate across queries
    avg_mae = sum([r['mae'] for r in results.values()]) / len(results)
    avg_rel_error = sum([r['relative_error'] for r in results.values()]) / len(results)
    avg_latency_overhead = sum([r['latency_overhead_pct'] for r in results.values()]) / len(results)
    avg_kanon_latency = sum([r['kanon_latency_ms'] for r in results.values()]) / len(results)
    
    return {
        'mae': avg_mae,
        'relative_error': avg_rel_error,
        'latency_ms': avg_kanon_latency,
        'latency_overhead_pct': avg_latency_overhead,
        'per_query': results
    }

def create_k_anonymous_table(conn, k_value, age_bucket=5):
    """
    Create k-anonymous version of adult_raw table
    Returns: number of suppressed rows, utility metrics
    """
    print(f"\n>>> Creating k-anonymous table (k={k_value}, age_bucket={age_bucket})...")
    
    # Read data
    query = "SELECT * FROM adult_raw_100000"
    df = pd.read_sql(query, conn)
    
    original_count = len(df)
    print(f"  Original rows: {original_count}")
    
    # Apply generalizations
    df['age_generalized'] = df['age'].apply(lambda x: generalize_age(x, age_bucket))
    df['education_generalized'] = df['education'].apply(generalize_education)
    
    # Define quasi-identifiers (attributes that could identify individuals)
    quasi_identifiers = ['age_generalized', 'education_generalized', 'sex', 'race']
    
    # Check initial k-anonymity
    satisfies, min_size, violations = check_k_anonymity(df, quasi_identifiers, k_value)
    print(f"  Before suppression: min_group={min_size}, violations={violations}")
    
    # Suppress small groups
    df_suppressed, suppressed_count = suppress_small_groups(df, quasi_identifiers, k_value)
    
    # Verify k-anonymity
    satisfies, min_size, violations = check_k_anonymity(df_suppressed, quasi_identifiers, k_value)
    print(f"  After suppression: satisfies_k={satisfies}, min_group={min_size}")
    print(f"  Suppressed {suppressed_count} rows ({suppressed_count/original_count*100:.1f}%)")
    
    # Create table in database
    table_name = f'adult_kanon_k{k_value}'
    
    cur = conn.cursor()
    
    # Drop if exists
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Create table
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            age_generalized INT,
            education_generalized TEXT,
            sex TEXT,
            race TEXT,
            workclass TEXT,
            occupation TEXT,
            marital_status TEXT,
            relationship TEXT,
            capital_gain INT,
            capital_loss INT,
            hours_per_week INT,
            native_country TEXT,
            income TEXT
        )
    """)
    
    # Insert data
    for idx, row in df_suppressed.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} 
            (age_generalized, education_generalized, sex, race, workclass, 
             occupation, marital_status, relationship, capital_gain, capital_loss,
             hours_per_week, native_country, income)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['age_generalized']),
            row['education_generalized'],
            row['sex'],
            row['race'],
            row['workclass'],
            row['occupation'],
            row['marital_status'],
            row['relationship'],
            int(row['capital_gain']),
            int(row['capital_loss']),
            int(row['hours_per_week']),
            row['native_country'],
            row['income']
        ))
        
        if idx % 10000 == 0:
            conn.commit()
    
    conn.commit()
    
    # Create indexes
    cur.execute(f"CREATE INDEX idx_{table_name}_age ON {table_name}(age_generalized)")
    cur.execute(f"CREATE INDEX idx_{table_name}_edu ON {table_name}(education_generalized)")
    conn.commit()  # commit before vacuum

    # VACUUM must run outside a transaction
    conn.autocommit = True
    cur.execute(f"VACUUM ANALYZE {table_name}")
    conn.autocommit = False
        
    conn.commit()
    cur.close()
    
    print(f"  ✓ Created table: {table_name}")
    
    # Create comparison table with generalized columns for fair comparison
    comparison_table = f'adult_raw_generalized_k{k_value}'
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {comparison_table}")
    
    # Create generalized version of original (without suppression) for comparison
    cur.execute(f"""
        CREATE TABLE {comparison_table} AS
        SELECT 
            id,
            (age / {age_bucket}) * {age_bucket} as age_generalized,
            CASE 
                WHEN education IN ('Preschool', '1st-4th', '5th-6th', '7th-8th') THEN 'Primary'
                WHEN education IN ('9th', '10th', '11th', '12th', 'HS-grad') THEN 'High-School'
                WHEN education IN ('Some-college', 'Assoc-voc', 'Assoc-acdm', 'Bachelors') THEN 'College'
                WHEN education IN ('Masters', 'Prof-school', 'Doctorate') THEN 'Graduate'
                ELSE 'Other'
            END as education_generalized,
            sex, race, workclass, occupation, marital_status, relationship,
            capital_gain, capital_loss, hours_per_week, native_country, income
        FROM adult_raw_100000
    """)
    conn.commit()
    cur.close()
    
    # Calculate utility metrics
    print(f"  Measuring utility metrics...")
    metrics = compute_aggregate_metrics(conn, comparison_table, table_name)
    
    return {
        'k': k_value,
        'table_name': table_name,
        'original_rows': original_count,
        'final_rows': len(df_suppressed),
        'suppressed_rows': suppressed_count,
        'suppression_rate': suppressed_count / original_count,
        'mae': metrics['mae'],
        'relative_error': metrics['relative_error'],
        'latency_ms': metrics['latency_ms'],
        'latency_overhead_pct': metrics['latency_overhead_pct']
    }

def main():
    parser = argparse.ArgumentParser(description='Generate k-anonymous tables')
    parser.add_argument('--k-values', nargs='+', type=int, default=[2, 5, 10, 20],
                       help='K values to generate')
    parser.add_argument('--age-bucket', type=int, default=5,
                       help='Age generalization bucket size')
    args = parser.parse_args()
    
    print("=== k-Anonymity Table Generation ===")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    results = []
    for k in args.k_values:
        result = create_k_anonymous_table(conn, k, args.age_bucket)
        results.append(result)
    
    conn.close()
    
    # Save detailed results
    output_dir = Path('results/raw')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'k_anonymity_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    print("\n" + "="*80)
    print("=== Summary: k-Anonymity Results ===")
    print("="*80)
    print(f"{'k':<5} {'Suppr.':<10} {'MAE':<10} {'Rel.Err.':<12} {'Latency':<12} {'Overhead':<10}")
    print(f"{'':5} {'(%)':<10} {'':<10} {'(%)':<12} {'(ms)':<12} {'(%)':<10}")
    print("-" * 80)
    for r in results:
        print(f"{r['k']:<5} "
              f"{r['suppression_rate']*100:<10.1f} "
              f"{r['mae']:<10.2f} "
              f"{r['relative_error']*100:<12.1f} "
              f"{r['latency_ms']:<12.2f} "
              f"{r['latency_overhead_pct']:<10.1f}")
    print("="*80)
    
    print(f"\n✓ All k-anonymous tables created!")
    print(f"✓ Detailed results saved to {output_dir / 'k_anonymity_results.json'}")

if __name__ == '__main__':
    main()

def generalize_age(age, granularity=5):
    """Generalize age to buckets"""
    return (age // granularity) * granularity

def generalize_education(education):
    """Map education to broader categories"""
    mapping = {
        'Preschool': 'Primary',
        '1st-4th': 'Primary',
        '5th-6th': 'Primary',
        '7th-8th': 'Middle',
        '9th': 'High-School',
        '10th': 'High-School',
        '11th': 'High-School',
        '12th': 'High-School',
        'HS-grad': 'High-School',
        'Some-college': 'College',
        'Assoc-voc': 'College',
        'Assoc-acdm': 'College',
        'Bachelors': 'College',
        'Masters': 'Graduate',
        'Prof-school': 'Graduate',
        'Doctorate': 'Graduate'
    }
    return mapping.get(education.strip(), 'Other')

def check_k_anonymity(df, quasi_identifiers, k):
    """
    Check if dataframe satisfies k-anonymity
    Returns: (satisfies_k, min_group_size, violating_groups)
    """
    grouped = df.groupby(quasi_identifiers).size()
    min_size = grouped.min()
    violations = grouped[grouped < k]
    
    return min_size >= k, min_size, len(violations)

def suppress_small_groups(df, quasi_identifiers, k):
    """Remove rows that belong to groups smaller than k"""
    group_sizes = df.groupby(quasi_identifiers).size()
    valid_groups = group_sizes[group_sizes >= k].index
    
    # Create boolean mask for valid rows
    mask = df.set_index(quasi_identifiers).index.isin(valid_groups)
    suppressed_df = df[mask].reset_index(drop=True)
    
    return suppressed_df, len(df) - len(suppressed_df)

def create_k_anonymous_table(conn, k_value, age_bucket=5):
    """
    Create k-anonymous version of adult_raw table
    Returns: number of suppressed rows, utility metrics
    """
    print(f"\n>>> Creating k-anonymous table (k={k_value}, age_bucket={age_bucket})...")
    
    # Read data
    query = "SELECT * FROM adult_raw_100000"
    df = pd.read_sql(query, conn)
    
    original_count = len(df)
    print(f"  Original rows: {original_count}")
    
    # Apply generalizations
    df['age_generalized'] = df['age'].apply(lambda x: generalize_age(x, age_bucket))
    df['education_generalized'] = df['education'].apply(generalize_education)
    
    # Define quasi-identifiers (attributes that could identify individuals)
    quasi_identifiers = ['age_generalized', 'education_generalized', 'sex', 'race']
    
    # Check initial k-anonymity
    satisfies, min_size, violations = check_k_anonymity(df, quasi_identifiers, k_value)
    print(f"  Before suppression: min_group={min_size}, violations={violations}")
    
    # Suppress small groups
    df_suppressed, suppressed_count = suppress_small_groups(df, quasi_identifiers, k_value)
    
    # Verify k-anonymity
    satisfies, min_size, violations = check_k_anonymity(df_suppressed, quasi_identifiers, k_value)
    print(f"  After suppression: satisfies_k={satisfies}, min_group={min_size}")
    print(f"  Suppressed {suppressed_count} rows ({suppressed_count/original_count*100:.1f}%)")
    
    # Create table in database
    table_name = f'adult_kanon_k{k_value}'
    
    cur = conn.cursor()
    
    # Drop if exists
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Create table
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            age_generalized INT,
            education_generalized TEXT,
            sex TEXT,
            race TEXT,
            workclass TEXT,
            occupation TEXT,
            marital_status TEXT,
            relationship TEXT,
            capital_gain INT,
            capital_loss INT,
            hours_per_week INT,
            native_country TEXT,
            income TEXT
        )
    """)
    
    # Insert data
    for idx, row in df_suppressed.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} 
            (age_generalized, education_generalized, sex, race, workclass, 
             occupation, marital_status, relationship, capital_gain, capital_loss,
             hours_per_week, native_country, income)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['age_generalized']),
            row['education_generalized'],
            row['sex'],
            row['race'],
            row['workclass'],
            row['occupation'],
            row['marital_status'],
            row['relationship'],
            int(row['capital_gain']),
            int(row['capital_loss']),
            int(row['hours_per_week']),
            row['native_country'],
            row['income']
        ))
        
        if idx % 10000 == 0:
            conn.commit()
    
    conn.commit()
    
    # Create indexes
    cur.execute(f"CREATE INDEX idx_{table_name}_age ON {table_name}(age_generalized)")
    cur.execute(f"CREATE INDEX idx_{table_name}_edu ON {table_name}(education_generalized)")
    conn.commit()  # commit before vacuum

    # VACUUM must run outside a transaction
    conn.autocommit = True
    cur.execute(f"VACUUM ANALYZE {table_name}")
    conn.autocommit = False
    conn.commit()
    cur.close()
    
    print(f"  ✓ Created table: {table_name}")
    
    # Calculate utility metrics
    metrics = calculate_utility_metrics(df, df_suppressed, quasi_identifiers)
    
    return {
        'k': k_value,
        'table_name': table_name,
        'original_rows': original_count,
        'final_rows': len(df_suppressed),
        'suppressed_rows': suppressed_count,
        'suppression_rate': suppressed_count / original_count,
        'metrics': metrics
    }

def calculate_utility_metrics(df_original, df_kanon, quasi_identifiers):
    """Calculate information loss metrics"""
    
    # Cardinality reduction
    original_unique = {qi: df_original[qi].nunique() for qi in quasi_identifiers}
    kanon_unique = {qi: df_kanon[qi].nunique() for qi in quasi_identifiers}
    
    cardinality_loss = {
        qi: 1 - (kanon_unique[qi] / original_unique[qi])
        for qi in quasi_identifiers
    }
    
    return {
        'cardinality_loss': cardinality_loss,
        'original_cardinality': original_unique,
        'kanon_cardinality': kanon_unique
    }

def main():
    parser = argparse.ArgumentParser(description='Generate k-anonymous tables')
    parser.add_argument('--k-values', nargs='+', type=int, default=[2, 5, 10, 20],
                       help='K values to generate')
    parser.add_argument('--age-bucket', type=int, default=5,
                       help='Age generalization bucket size')
    args = parser.parse_args()
    
    print("=== k-Anonymity Table Generation ===")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    results = []
    for k in args.k_values:
        result = create_k_anonymous_table(conn, k, args.age_bucket)
        results.append(result)
    
    conn.close()
    
    # Print summary
    print("\n=== Summary ===")
    print(f"{'k':<5} {'Table':<20} {'Rows':<10} {'Suppressed':<12} {'Rate':<8}")
    print("-" * 60)
    for r in results:
        print(f"{r['k']:<5} {r['table_name']:<20} {r['final_rows']:<10} "
              f"{r['suppressed_rows']:<12} {r['suppression_rate']*100:<8.1f}%")
    
    print("\n✓ All k-anonymous tables created!")

if __name__ == '__main__':
    main()
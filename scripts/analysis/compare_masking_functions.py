#!/usr/bin/env python3

import psycopg2
import pandas as pd
import json
import time
import statistics
from pathlib import Path
from scipy.stats import entropy
from scipy.spatial.distance import jensenshannon

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'benchmark',
    'user': 'postgres',
    'password': 'postgres'
}

MASKING_TABLES = {
    'original': 'mask_test_data',
    'hash': 'adult_mask_hash',
    'partial': 'adult_mask_partial',
    'shuffle': 'adult_mask_shuffle',
    'fake': 'adult_mask_anon_fake',
    'noise': 'adult_mask_noise',
    'generalize': 'adult_mask_generalize'
}

TEST_QUERIES = {
    'point_lookup': "SELECT * FROM {table} WHERE id = 12345",
    'range_scan': "SELECT COUNT(*) FROM {table} WHERE age BETWEEN 30 AND 50",
    'group_by': "SELECT occupation, COUNT(*) FROM {table} GROUP BY occupation",
    'join_aggregate': """
        SELECT m.education, COUNT(*) as cnt, AVG(m.hours_per_week) as avg_hours
        FROM {table} m
        GROUP BY m.education
    """
}

def measure_query_latency(conn, query, runs=5, warmup=2):
    cur = conn.cursor()
    
    for _ in range(warmup):
        cur.execute(query)
        cur.fetchall()
    
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        cur.execute(query)
        cur.fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    cur.close()
    return statistics.median(times)

def get_table_storage(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT 
            pg_total_relation_size('{table_name}') as total_bytes,
            pg_relation_size('{table_name}') as table_bytes,
            pg_indexes_size('{table_name}') as index_bytes
    """)
    result = cur.fetchone()
    cur.close()
    
    if result:
        return {
            'total_mb': result[0] / (1024 * 1024),
            'table_mb': result[1] / (1024 * 1024),
            'index_mb': result[2] / (1024 * 1024)
        }
    return None

def calculate_cardinality_preservation(conn, original_table, masked_table, column):

    cur = conn.cursor()
    
    cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {original_table}")
    original_cardinality = cur.fetchone()[0]
    
    cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {masked_table}")
    masked_cardinality = cur.fetchone()[0]
    
    cur.close()
    
    if original_cardinality == 0:
        return 0
    
    return masked_cardinality / original_cardinality

def calculate_distribution_distance(conn, original_table, masked_table, column):

    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT {column}, COUNT(*) as cnt
        FROM {original_table}
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        ORDER BY {column}
    """)
    original_dist = dict(cur.fetchall())
    
    cur.execute(f"""
        SELECT {column}, COUNT(*) as cnt
        FROM {masked_table}
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        ORDER BY {column}
    """)
    masked_dist = dict(cur.fetchall())
    
    cur.close()
    
    all_values = set(original_dist.keys()) | set(masked_dist.keys())
    
    total_original = sum(original_dist.values())
    total_masked = sum(masked_dist.values())
    
    p = [original_dist.get(v, 0) / total_original for v in sorted(all_values)]
    q = [masked_dist.get(v, 0) / total_masked for v in sorted(all_values)]
    
    try:
        js_div = jensenshannon(p, q)
        return float(js_div)
    except:
        return float('nan')

def test_index_compatibility(conn, table_name, column):

    cur = conn.cursor()
    
    query = f"""
        EXPLAIN (FORMAT JSON) 
        SELECT * FROM {table_name} WHERE {column} = 'some_value'
    """
    
    try:
        cur.execute(query)
        plan = cur.fetchone()[0][0]
        
        plan_str = json.dumps(plan).lower()
        uses_index = 'index scan' in plan_str or 'index only scan' in plan_str
        
        cur.close()
        return uses_index
    except:
        cur.close()
        return False

def compare_masking_functions():
    print("=== Comparing Masking Functions ===\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    results = []
    
    original_table = MASKING_TABLES['original']
    
    for mask_name, table_name in MASKING_TABLES.items():
        print(f">>> Analyzing: {mask_name}")
        
        result = {
            'masking_function': mask_name,
            'table': table_name
        }
        
        print("  - Measuring query latency...")
        for query_name, query_template in TEST_QUERIES.items():
            query = query_template.format(table=table_name)
            try:
                latency = measure_query_latency(conn, query, runs=5)
                result[f'latency_{query_name}_ms'] = round(latency, 2)
            except Exception as e:
                print(f"    Warning: {query_name} failed: {e}")
                result[f'latency_{query_name}_ms'] = None
        
        print("  - Measuring storage...")
        storage = get_table_storage(conn, table_name)
        if storage:
            result['storage_total_mb'] = round(storage['total_mb'], 2)
            result['storage_table_mb'] = round(storage['table_mb'], 2)
            result['storage_index_mb'] = round(storage['index_mb'], 2)
        
        if mask_name != 'original':
            print("  - Calculating utility preservation...")
            
            try:
                card_occupation = calculate_cardinality_preservation(
                    conn, original_table, table_name, 'occupation'
                )
                result['cardinality_ratio_occupation'] = round(card_occupation, 3)
            except:
                result['cardinality_ratio_occupation'] = None
            
            try:
                js_div = calculate_distribution_distance(
                    conn, original_table, table_name, 'occupation'
                )
                result['js_divergence_occupation'] = round(js_div, 3)
            except:
                result['js_divergence_occupation'] = None
            
            try:
                uses_index = test_index_compatibility(conn, table_name, 'occupation')
                result['index_compatible'] = 'Yes' if uses_index else 'No'
            except:
                result['index_compatible'] = 'Unknown'
        
        results.append(result)
        print(f"  ✓ Complete\n")
    
    conn.close()
    
    df_results = pd.DataFrame(results)
    
    original_latencies = df_results[df_results['masking_function'] == 'original'].iloc[0]
    
    for query_name in ['point_lookup', 'range_scan', 'group_by', 'join_aggregate']:
        col_name = f'latency_{query_name}_ms'
        if col_name in df_results.columns:
            baseline = original_latencies[col_name]
            df_results[f'overhead_{query_name}_pct'] = (
                (df_results[col_name] - baseline) / baseline * 100
            ).round(1)
    
    output_dir = Path('results/raw')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    df_results.to_csv(output_dir / 'masking_function_comparison.csv', index=False)
    df_results.to_json(output_dir / 'masking_function_comparison.json', orient='records', indent=2)
    
    print("\n=== Summary Table ===\n")
    
    summary_cols = [
        'masking_function',
        'latency_group_by_ms',
        'overhead_group_by_pct',
        'storage_total_mb',
        'cardinality_ratio_occupation',
        'js_divergence_occupation',
        'index_compatible'
    ]
    
    summary_df = df_results[summary_cols].copy()
    summary_df.columns = [
        'Function',
        'Latency (ms)',
        'Overhead (%)',
        'Storage (MB)',
        'Cardinality Ratio',
        'JS Divergence',
        'Index Compatible'
    ]
    
    print(summary_df.to_string(index=False))
    print("\n")
    
    summary_df.to_csv(output_dir / 'masking_function_summary.csv', index=False)
    summary_df.to_latex(output_dir / 'masking_function_summary.tex', index=False)
    
    print(f"✓ Results saved to {output_dir}")
    print(f"  - masking_function_comparison.csv (full)")
    print(f"  - masking_function_summary.csv (for paper)")
    print(f"  - masking_function_summary.tex (LaTeX table)")

if __name__ == '__main__':
    compare_masking_functions()
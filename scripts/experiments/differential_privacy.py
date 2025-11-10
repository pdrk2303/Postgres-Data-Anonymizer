#!/usr/bin/env python3
import psycopg2
import numpy as np
import pandas as pd
import json
import argparse
from pathlib import Path

np.random.seed(42)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'benchmark',
    'user': 'postgres',
    'password': 'postgres'
}

def laplace_mechanism(true_value, sensitivity, epsilon):
    scale = sensitivity / epsilon
    noise = np.random.laplace(0, scale)
    return true_value + noise

def run_dp_aggregate_queries(conn, epsilon_values):
    """
    Queries tested:
    1. COUNT(*) by age bucket
    2. AVG(capital_gain) by education
    3. SUM(hours_per_week) by occupation
    """
    
    results = []
    
    print("\n>>> Query 1: COUNT by age bucket")
    query1 = """
        SELECT (age / 10) * 10 as age_bucket, COUNT(*) as cnt
        FROM adult_raw_100000
        GROUP BY age_bucket
        ORDER BY age_bucket
    """
    
    df1 = pd.read_sql(query1, conn)
    
    for epsilon in epsilon_values:
        sensitivity = 1
        
        df1_noised = df1.copy()
        df1_noised['cnt_noised'] = df1['cnt'].apply(
            lambda x: max(0, laplace_mechanism(x, sensitivity, epsilon))  # Clamp to non-negative
        )
        
        mae = np.mean(np.abs(df1_noised['cnt'] - df1_noised['cnt_noised']))
        rel_error = np.mean(np.abs(df1_noised['cnt'] - df1_noised['cnt_noised']) / df1_noised['cnt'])
        
        results.append({
            'query': 'count_by_age',
            'epsilon': epsilon,
            'sensitivity': sensitivity,
            'mae': mae,
            'relative_error': rel_error,
            'results': df1_noised.to_dict('records')
        })
        
        print(f"  ε={epsilon}: MAE={mae:.2f}, RelError={rel_error*100:.1f}%")
    
    print("\n>>> Query 2: AVG(capital_gain) by education")
    query2 = """
        SELECT education, AVG(capital_gain) as avg_gain, COUNT(*) as cnt
        FROM adult_raw_100000
        WHERE education != '?'
        GROUP BY education
        ORDER BY avg_gain DESC
    """
    
    df2 = pd.read_sql(query2, conn)
    
    max_gain = 100000 
    
    for epsilon in epsilon_values:
        df2_noised = df2.copy()
        
        sum_noised = (df2['avg_gain'] * df2['cnt']).apply(
            lambda x: laplace_mechanism(x, max_gain, epsilon/2)  # Split epsilon budget
        )
        
        cnt_noised = df2['cnt'].apply(
            lambda x: max(1, laplace_mechanism(x, 1, epsilon/2))  # Clamp to at least 1
        )
        
        df2_noised['avg_gain_noised'] = sum_noised / cnt_noised
        
        mae = np.mean(np.abs(df2_noised['avg_gain'] - df2_noised['avg_gain_noised']))
        rel_error = np.mean(np.abs(df2_noised['avg_gain'] - df2_noised['avg_gain_noised']) / 
                           (df2_noised['avg_gain'] + 1))  # +1 to avoid div by zero
        
        results.append({
            'query': 'avg_capital_gain_by_education',
            'epsilon': epsilon,
            'sensitivity': f'sum:{max_gain}, count:1',
            'mae': mae,
            'relative_error': rel_error,
            'results': df2_noised.to_dict('records')
        })
        
        print(f"  ε={epsilon}: MAE={mae:.2f}, RelError={rel_error*100:.1f}%")
    
    print("\n>>> Query 3: SUM(hours_per_week) by occupation")
    query3 = """
        SELECT occupation, SUM(hours_per_week) as total_hours
        FROM adult_raw_100000
        WHERE occupation != '?'
        GROUP BY occupation
        ORDER BY total_hours DESC
        LIMIT 10
    """
    
    df3 = pd.read_sql(query3, conn)
    
    max_hours = 100 
    
    for epsilon in epsilon_values:
        sensitivity = max_hours
        
        df3_noised = df3.copy()
        df3_noised['total_hours_noised'] = df3['total_hours'].apply(
            lambda x: max(0, laplace_mechanism(x, sensitivity, epsilon))
        )
        
        mae = np.mean(np.abs(df3_noised['total_hours'] - df3_noised['total_hours_noised']))
        rel_error = np.mean(np.abs(df3_noised['total_hours'] - df3_noised['total_hours_noised']) / 
                           df3_noised['total_hours'])
        
        results.append({
            'query': 'sum_hours_by_occupation',
            'epsilon': epsilon,
            'sensitivity': sensitivity,
            'mae': mae,
            'relative_error': rel_error,
            'results': df3_noised.to_dict('records')
        })
        
        print(f"  ε={epsilon}: MAE={mae:.2f}, RelError={rel_error*100:.1f}%")
    
    return results

def main():
    parser = argparse.ArgumentParser(description='Differential Privacy simulation')
    parser.add_argument('--epsilon', nargs='+', type=float, default=[0.1, 0.5, 1.0, 5.0],
                       help='Epsilon values to test')
    args = parser.parse_args()
    
    print("=== Differential Privacy Simulation ===")
    print(f"Testing ε values: {args.epsilon}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    results = run_dp_aggregate_queries(conn, args.epsilon)
    
    conn.close()
    
    output_dir = Path('results/raw')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / 'dp_results.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n✓ Results saved: {output_file}")
    
    print("\n=== Summary: Accuracy vs Privacy ===")
    print(f"{'Query':<30} {'ε':<8} {'MAE':<12} {'Rel Error':<12}")
    print("-" * 65)
    
    for r in results:
        print(f"{r['query']:<30} {r['epsilon']:<8} {r['mae']:<12.2f} {r['relative_error']*100:<12.1f}%")

if __name__ == '__main__':

    main()

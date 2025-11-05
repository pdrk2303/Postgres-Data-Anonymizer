#!/usr/bin/env python3
"""
Deep-dive analysis of query plans
Shows how masking affects PostgreSQL query planner decisions

Why: Masking might prevent index usage or change join strategies
"""

import json
import psycopg2
from pathlib import Path
from collections import defaultdict

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'benchmark',
    'user': 'postgres',
    'password': 'postgres'
}

def extract_plan_features(plan_node):
    """Recursively extract key features from EXPLAIN plan"""
    features = {
        'node_type': plan_node.get('Node Type', 'Unknown'),
        'actual_rows': plan_node.get('Actual Rows', 0),
        'plan_rows': plan_node.get('Plan Rows', 0),
        'actual_time': plan_node.get('Actual Total Time', 0),
        'shared_hit_blocks': plan_node.get('Shared Hit Blocks', 0),
        'shared_read_blocks': plan_node.get('Shared Read Blocks', 0)
    }
    
    # Check for index usage
    if 'Index' in features['node_type']:
        features['uses_index'] = True
        features['index_name'] = plan_node.get('Index Name', '')
    else:
        features['uses_index'] = False
    
    # Recurse into child plans
    if 'Plans' in plan_node:
        features['children'] = [extract_plan_features(child) for child in plan_node['Plans']]
    
    return features

def get_plan_for_query(conn, query, table_name):
    """Get EXPLAIN plan for a query"""
    cur = conn.cursor()
    
    explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query.format(table=table_name)}"
    
    try:
        cur.execute(explain_query)
        plan = cur.fetchone()[0][0]
        cur.close()
        return plan
    except Exception as e:
        cur.close()
        return None

def compare_plans():
    """Compare query plans across systems"""
    
    print("=== Query Plan Analysis ===\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Test queries
    queries = {
        'point_lookup': "SELECT * FROM {table} WHERE id = 5000",
        'index_scan': "SELECT * FROM {table} WHERE age = 35",
        'seq_scan': "SELECT COUNT(*) FROM {table} WHERE age BETWEEN 25 AND 45",
        'group_by': "SELECT education, COUNT(*) FROM {table} GROUP BY education"
    }
    
    systems = {
        'raw': 'adult_raw_100000',
        'view': 'adult_masked_view',
        'static': 'adult_static_masked'
    }
    
    results = defaultdict(dict)
    
    for query_name, query_template in queries.items():
        print(f">>> Query: {query_name}")
        
        for system_name, table_name in systems.items():
            query = query_template.format(table=table_name)
            plan = get_plan_for_query(conn, query, table_name)
            
            if plan:
                features = extract_plan_features(plan['Plan'])
                
                # Key metrics
                uses_index = features.get('uses_index', False)
                node_type = features['node_type']
                row_estimate_error = abs(features['plan_rows'] - features['actual_rows']) / max(features['actual_rows'], 1)
                
                results[query_name][system_name] = {
                    'node_type': node_type,
                    'uses_index': uses_index,
                    'actual_time_ms': features['actual_time'],
                    'row_estimate_error': row_estimate_error,
                    'buffer_hits': features['shared_hit_blocks']
                }
                
                print(f"  {system_name:10s}: {node_type:20s} "
                      f"{'[INDEX]' if uses_index else '[SEQ]':8s} "
                      f"{features['actual_time']:.2f}ms")
            else:
                print(f"  {system_name:10s}: ERROR")
        
        print()
    
    conn.close()
    
    # Save detailed results
    output_dir = Path('results/raw')
    with open(output_dir / 'query_plan_analysis.json', 'w') as f:
        json.dump(dict(results), f, indent=2, default=str)
    
    # Print summary insights
    print("\n=== Key Findings ===\n")
    
    # Check if masking prevents index usage
    for query_name, systems_data in results.items():
        raw_uses_index = systems_data.get('raw', {}).get('uses_index', False)
        
        for system in ['view', 'static']:
            if system in systems_data:
                system_uses_index = systems_data[system].get('uses_index', False)
                
                if raw_uses_index and not system_uses_index:
                    print(f"⚠  {query_name}: {system} doesn't use index (raw does)")
                elif raw_uses_index == system_uses_index:
                    print(f"✓  {query_name}: {system} preserves index usage")
    
    print()
    
    # Check row estimate errors
    print("Row Estimate Errors (planner accuracy):")
    for query_name, systems_data in results.items():
        print(f"  {query_name}:")
        for system, data in systems_data.items():
            error = data['row_estimate_error']
            print(f"    {system:10s}: {error:.2%}")
    
    print(f"\n✓ Detailed results: {output_dir / 'query_plan_analysis.json'}")

if __name__ == '__main__':
    compare_plans()
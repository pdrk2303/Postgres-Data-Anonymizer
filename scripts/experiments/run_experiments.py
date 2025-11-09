#!/usr/bin/env python3
"""
Run benchmarking experiments across baselines and system under test
Collects: latency, EXPLAIN ANALYZE plans, pg_stat_statements metrics, storage sizes

Usage: python scripts/run_experiments.py --config experiments_100k.yaml
"""

import psycopg2
import psycopg2.extras
import json
import time
import statistics
import argparse
import yaml
from pathlib import Path
from datetime import datetime

DB_CONFIGS = {
    'postgres': {'user': 'postgres', 'password': 'postgres'},
    'analyst': {'user': 'analyst', 'password': 'analyst'},
    'masked_user': {'user': 'masked_user', 'password': 'masked'}
}

class BenchmarkRunner:
    def __init__(self, config_file):
        with open(config_file) as f:
            self.config = yaml.safe_load(f)
        
        self.results_dir = Path('results/raw')
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def get_connection(self, role='postgres'):
        """Get database connection for specific role"""
        db_config = DB_CONFIGS[role].copy()
        db_config.update({
            'host': 'localhost',
            'port': 5432,
            'database': 'benchmark'
        })
        return psycopg2.connect(**db_config)
    
    def clear_caches(self, conn):
        """Clear PostgreSQL caches for cold-cache experiments"""
        cur = conn.cursor()
        # Discard query cache
        cur.execute("DISCARD ALL")
        # Request kernel to drop page cache (requires docker privileged mode)
        # For now, we rely on DISCARD ALL and restarting queries
        cur.close()
    
    def warmup_query(self, conn, query, runs=3):
        """Run query N times to warm up caches"""
        cur = conn.cursor()
        for _ in range(runs):
            cur.execute(query)
            cur.fetchall()
        cur.close()
    
    def run_query_with_explain(self, conn, query, role='postgres'):
        """Execute query with EXPLAIN ANALYZE and return metrics"""
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        plan = None
        planning_time_ms = 0
        execution_time_db = 0

        if role != 'masked_user':
            # Get execution plan (only for unmasked users)
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
            cur.execute(explain_query)
            plan_result = cur.fetchone()['QUERY PLAN'][0]
            plan = plan_result
            execution_time_db = plan_result['Execution Time']
            planning_time_ms = plan_result['Planning Time']
        
        # Measure wall-clock time (client-side)
        start = time.perf_counter()
        cur.execute(query)
        results = cur.fetchall()
        end = time.perf_counter()
        
        cur.close()

        return {
            'wall_clock_ms': (end - start) * 1000,
            'execution_time_ms': execution_time_db if execution_time_db else None,
            'planning_time_ms': planning_time_ms,
            'plan': plan,
            'row_count': len(results)
        }
    
    def get_table_size(self, conn, table_name):
        """Get table and index sizes (or note if view)"""
        cur = conn.cursor()
        
        # Check if it's a view
        cur.execute(f"""
            SELECT relkind FROM pg_class WHERE relname = '{table_name}'
        """)
        rel_info = cur.fetchone()
        
        if not rel_info:
            cur.close()
            return None
        
        relkind = rel_info[0]
        
        if relkind == 'v':  # It's a view
            cur.close()
            return {
                'total_size': '0 bytes (view)',
                'table_size': '0 bytes (view)',
                'index_size': '0 bytes (view)',
                'total_bytes': 0,
                'is_view': True
            }
        
        # It's a table - get actual size
        cur.execute(f"""
            SELECT 
                pg_size_pretty(pg_total_relation_size('{table_name}')) as total_size,
                pg_size_pretty(pg_relation_size('{table_name}')) as table_size,
                pg_size_pretty(pg_indexes_size('{table_name}')) as index_size,
                pg_total_relation_size('{table_name}') as total_bytes
        """)
        result = cur.fetchone()
        cur.close()
        
        if result:
            return {
                'total_size': result[0],
                'table_size': result[1],
                'index_size': result[2],
                'total_bytes': result[3],
                'is_view': False
            }
        return None
    
    def run_experiment(self, exp_config):
        """Run single experiment configuration"""
        exp_name = exp_config['name']
        system_type = exp_config['system']  # 'raw', 'view', 'dynamic', 'static'
        workload_file = exp_config['workload']
        role = exp_config.get('role', 'postgres')
        num_runs = exp_config.get('runs', 5)
        warmup_runs = exp_config.get('warmup', 3)
        cold_cache = exp_config.get('cold_cache', False)
        
        print(f"\n>>> Running: {exp_name} (system={system_type}, role={role})")
        
        # Load workload SQL
        with open(f'workloads/{workload_file}') as f:
            queries = [q.strip() for q in f.read().split(';') if q.strip()]
        
        # Map system type to table name
        table_map = {
            'raw': {'adult': 'adult_raw_100000', 'healthcare': 'healthcare_raw_100000'},
            'view': {'adult': 'adult_masked_view', 'healthcare': 'healthcare_view_masked'},
            'static': {'adult': 'adult_static_masked', 'healthcare': 'healthcare_static_masked'},
            'dynamic': {'adult': 'adult_raw_100000', 'healthcare': 'healthcare_raw_100000'}  # dynamic uses raw tables with role masking
        }
        
        results = []
        
        conn = self.get_connection(role)
        
        for query_idx, query_template in enumerate(queries):
            # Replace table references based on system type
            query = query_template
            for dataset in ['adult', 'healthcare']:
                if dataset in query_template.lower():
                    target_table = table_map[system_type][dataset]
                    query = query.replace(f'{dataset}_raw', target_table)
            
            print(f"  Query {query_idx + 1}/{len(queries)}: ", end='', flush=True)
            
            # Warmup
            if not cold_cache:
                self.warmup_query(conn, query, warmup_runs)
            
            # Measured runs
            run_results = []
            for run in range(num_runs):
                if cold_cache:
                    self.clear_caches(conn)
                
                metrics = self.run_query_with_explain(conn, query, role)
                run_results.append(metrics)
                print('.', end='', flush=True)
            
            # Aggregate statistics
            wall_times = [r['wall_clock_ms'] for r in run_results]

            exec_times = [r['execution_time_ms'] for r in run_results if r['execution_time_ms'] is not None]
            planning_times = [r['planning_time_ms'] for r in run_results if r['planning_time_ms'] is not None]
        
            result = {
                'experiment': exp_name,
                'system': system_type,
                'role': role,
                'query_idx': query_idx,
                'query': query[:200],  # Truncate for readability
                'runs': num_runs,
                'wall_clock_median_ms': statistics.median(wall_times),
                'wall_clock_mean_ms': statistics.mean(wall_times),
                'wall_clock_std_ms': statistics.stdev(wall_times) if len(wall_times) > 1 else 0,
                'execution_median_ms': statistics.median(exec_times) if exec_times else None,
                'execution_mean_ms': statistics.mean(exec_times) if exec_times else None,
                'planning_median_ms': statistics.median(planning_times) if planning_times else None,
                'row_count': run_results[0]['row_count'],
                'explain_plan': run_results[0]['plan']  # Save one plan for analysis
            }
            
            results.append(result)
            print(f" ✓ {result['wall_clock_median_ms']:.2f}ms")
        
        conn.close()
        
        # Save results
        output_file = self.results_dir / f'{exp_name}_{self.timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"  Results saved: {output_file}")
        return results
    
    def collect_storage_metrics(self):
        """Collect storage sizes for all tables/views"""
        print("\n>>> Collecting storage metrics...")
        
        conn = self.get_connection('postgres')
        
        tables = [
            'adult_raw_100000', 'adult_view_masked', 'adult_static_masked',
            'healthcare_raw_100000', 'healthcare_view_masked', 'healthcare_static_masked'
        ]
        
        storage_results = {}
        for table in tables:
            size_info = self.get_table_size(conn, table)
            if size_info:
                storage_results[table] = size_info
                print(f"  {table}: {size_info['total_size']}")
        
        conn.close()
        
        # Save storage metrics
        output_file = self.results_dir / f'storage_metrics_{self.timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(storage_results, f, indent=2)
        
        return storage_results
    
    def run_all_experiments(self):
        """Run all experiments defined in config"""
        print(f"=== Starting Benchmark Run ({self.timestamp}) ===")
        print(f"Config: {self.config.get('description', 'No description')}")
        
        all_results = []
        
        for exp in self.config['experiments']:
            try:
                results = self.run_experiment(exp)
                all_results.extend(results)
            except Exception as e:
                print(f"  ✗ Error in {exp['name']}: {e}")
        
        # Collect storage metrics
        storage_metrics = self.collect_storage_metrics()
        
        # Save combined results
        summary_file = self.results_dir / f'summary_{self.timestamp}.json'
        with open(summary_file, 'w') as f:
            json.dump({
                'config': self.config,
                'results': all_results,
                'storage': storage_metrics,
                'timestamp': self.timestamp
            }, f, indent=2, default=str)
        
        print(f"\n✓ All experiments complete! Summary: {summary_file}")

def main():
    parser = argparse.ArgumentParser(description='Run PostgreSQL anonymizer benchmarks')
    parser.add_argument('--config', required=True, help='YAML config file')
    args = parser.parse_args()
    
    runner = BenchmarkRunner(args.config)
    runner.run_all_experiments()

if __name__ == '__main__':
    main()
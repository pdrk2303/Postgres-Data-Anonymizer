#!/usr/bin/env python3
"""
Concurrency benchmark using pgbench-style workload
Tests how masking overhead scales with concurrent clients

Why: Dynamic masking might have worse overhead under concurrent load
"""

import subprocess
import json
import re
from pathlib import Path
import pandas as pd

RESULTS_DIR = Path('results/raw')
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

SYSTEMS = {
    'raw': ('adult_raw_100000', 'postgres'),
    'view': ('adult_masked_view', 'postgres'),
    'static': ('adult_static_masked', 'postgres'),
    'dynamic': ('adult_raw_100000', 'masked_user')  # Dynamic masking via role
}

CLIENT_COUNTS = [1, 4, 8, 16]
DURATION = 30  # seconds

def create_pgbench_script(table_name):
    """Generate pgbench script for specific table"""
    script = f"""\\set id random(1, 100000)
\\set age1 random(20, 40)
\\set age2 random(40, 60)
SELECT * FROM {table_name} WHERE id = :id;
SELECT education, COUNT(*) FROM {table_name} WHERE age BETWEEN :age1 AND :age2 GROUP BY education LIMIT 5;
SELECT AVG(hours_per_week) FROM {table_name} WHERE age > :age1;
"""
    
    script_file = Path(f'/tmp/pgbench_{table_name}.sql')
    with open(script_file, 'w') as f:
        f.write(script)
    
    return script_file

def run_pgbench(table_name, role, num_clients):
    """Run pgbench with specified concurrency"""
    
    # Create custom script
    script_file = create_pgbench_script(table_name)
    
    # Copy script into Docker container
    subprocess.run([
        'docker', 'cp', str(script_file),
        f'anonpg_benchmark:/tmp/pgbench_{table_name}.sql'
    ], check=True)
    
    # Run pgbench
    cmd = [
        'docker', 'exec', 'anonpg_benchmark',
        'pgbench',
        '-U', role,
        '-d', 'benchmark',
        '-c', str(num_clients),      # Number of concurrent clients
        '-j', str(min(num_clients, 4)),  # Number of threads
        '-T', str(DURATION),         # Duration in seconds
        '-f', f'/tmp/pgbench_{table_name}.sql',
        '-r'                         # Report per-statement latencies
    ]
    
    print(f"    Running: {num_clients} clients, {DURATION}s duration...")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=DURATION + 30)
        
        # Parse output
        output = result.stdout + result.stderr
        
        # Extract TPS (transactions per second)
        tps_match = re.search(r'tps = ([\d.]+)', output)
        tps = float(tps_match.group(1)) if tps_match else 0
        
        # Extract average latency
        lat_match = re.search(r'latency average = ([\d.]+) ms', output)
        latency = float(lat_match.group(1)) if lat_match else 0
        
        return {
            'tps': tps,
            'latency_avg_ms': latency,
            'clients': num_clients,
            'success': True
        }
    
    except subprocess.TimeoutExpired:
        print(f"      ✗ Timeout")
        return {
            'tps': 0,
            'latency_avg_ms': 0,
            'clients': num_clients,
            'success': False,
            'error': 'timeout'
        }
    except Exception as e:
        print(f"      ✗ Error: {e}")
        return {
            'tps': 0,
            'latency_avg_ms': 0,
            'clients': num_clients,
            'success': False,
            'error': str(e)
        }

def main():
    print("=== Concurrency Benchmark (pgbench) ===\n")
    
    all_results = []
    
    for system_name, (table_name, role) in SYSTEMS.items():
        print(f">>> Testing: {system_name} (table={table_name}, role={role})")
        
        for num_clients in CLIENT_COUNTS:
            result = run_pgbench(table_name, role, num_clients)
            result['system'] = system_name
            result['table'] = table_name
            result['role'] = role
            
            all_results.append(result)
            
            if result['success']:
                print(f"      {num_clients} clients: {result['tps']:.1f} TPS, "
                      f"{result['latency_avg_ms']:.2f}ms latency")
            else:
                print(f"      {num_clients} clients: FAILED")
        
        print()
    
    # Save results
    df = pd.DataFrame(all_results)
    df.to_csv(RESULTS_DIR / 'concurrency_results.csv', index=False)
    df.to_json(RESULTS_DIR / 'concurrency_results.json', orient='records', indent=2)
    
    # Print summary table
    print("\n=== Concurrency Results Summary ===\n")
    
    pivot_tps = df[df['success']].pivot_table(
        index='clients',
        columns='system',
        values='tps',
        aggfunc='mean'
    )
    
    print("Throughput (TPS):")
    print(pivot_tps.to_string())
    print()
    
    pivot_lat = df[df['success']].pivot_table(
        index='clients',
        columns='system',
        values='latency_avg_ms',
        aggfunc='mean'
    )
    
    print("Average Latency (ms):")
    print(pivot_lat.to_string())
    print()
    
    # Calculate overhead
    if 'raw' in pivot_tps.columns:
        print("Throughput Overhead vs Raw (%):")
        overhead = (pivot_tps.div(pivot_tps['raw'], axis=0) - 1) * -100
        print(overhead.to_string())
    
    print(f"\n✓ Results saved to {RESULTS_DIR / 'concurrency_results.csv'}")

if __name__ == '__main__':
    main()
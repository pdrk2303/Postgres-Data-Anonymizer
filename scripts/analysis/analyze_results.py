#!/usr/bin/env python3
"""
Analyze benchmark results and generate plots for paper

Produces:
1. Latency comparison bar charts
2. Overhead percentage table
3. Query plan analysis
4. Privacy-utility trade-off curves
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 10

RESULTS_DIR = Path('results/raw')
PLOTS_DIR = Path('results/plots')
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def load_latest_summary():
    """Load most recent summary file"""
    summary_files = sorted(RESULTS_DIR.glob('summary_*.json'))
    if not summary_files:
        raise FileNotFoundError("No summary files found")
    
    latest = summary_files[-1]
    print(f"Loading: {latest}")
    
    with open(latest) as f:
        return json.load(f)

def plot_latency_comparison(df_results):
    """
    Plot 1: Latency comparison across systems
    Bar chart grouped by query type
    """
    print("\n>>> Generating Plot 1: Latency Comparison")
    
    # Extract workload name from experiment name
    df_results['workload'] = df_results['experiment'].str.extract(r'_(point_lookup|range|groupby|distinct|topk)')
    
    # Pivot for plotting
    pivot = df_results.pivot_table(
        index='workload',
        columns='system',
        values='wall_clock_median_ms',
        aggfunc='mean'
    )
    
    system_order = ['raw', 'view', 'static', 'dynamic']
    pivot = pivot[[col for col in system_order if col in pivot.columns]]
    
    ax = pivot.plot(kind='bar', width=0.8, figsize=(12, 6))
    ax.set_xlabel('Workload Type', fontsize=12, fontweight='bold')
    ax.set_ylabel('Median Latency (ms)', fontsize=12, fontweight='bold')
    ax.set_title('Query Latency Comparison Across Systems', fontsize=14, fontweight='bold')
    ax.legend(title='System', labels=['Raw (Baseline)', 'View Masking', 'Static Masked', 'Dynamic Masked'])
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'latency_comparison.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'latency_comparison.pdf')
    print(f"  Saved: {PLOTS_DIR / 'latency_comparison.png'}")
    plt.close()

def calculate_overhead_table(df_results):
    """
    Table: Overhead percentage vs baseline
    """
    print("\n>>> Generating Table: Overhead Analysis")
    
    # Group by workload and system
    grouped = df_results.groupby(['workload', 'system'])['wall_clock_median_ms'].mean().reset_index()
    
    # Get baseline (raw system)
    baseline = grouped[grouped['system'] == 'raw'].set_index('workload')['wall_clock_median_ms']
    
    # Calculate overhead for each system
    overhead_data = []
    for system in ['view', 'static', 'dynamic']:
        system_data = grouped[grouped['system'] == system].set_index('workload')['wall_clock_median_ms']
        overhead = ((system_data - baseline) / baseline * 100).round(1)
        
        for workload, ovh in overhead.items():
            overhead_data.append({
                'System': system,
                'Workload': workload,
                'Overhead (%)': ovh
            })
    
    df_overhead = pd.DataFrame(overhead_data)
    
    # Pivot for nice display
    pivot_overhead = df_overhead.pivot(index='Workload', columns='System', values='Overhead (%)')
    
    print("\n" + "="*60)
    print("Overhead vs Raw Baseline (%)")
    print("="*60)
    print(pivot_overhead.to_string())
    print("="*60)
    
    # Save to CSV
    pivot_overhead.to_csv(PLOTS_DIR / 'overhead_table.csv')
    print(f"\n  Saved: {PLOTS_DIR / 'overhead_table.csv'}")
    
    return pivot_overhead

def plot_privacy_utility_tradeoff():
    """
    Plot 2: Privacy-Utility Trade-off from DP results
    X-axis: epsilon, Y-axis: relative error
    """
    print("\n>>> Generating Plot 2: Privacy-Utility Trade-off")
    
    dp_file = RESULTS_DIR / 'dp_results.json'
    if not dp_file.exists():
        print("  Skipping: dp_results.json not found")
        return
    
    with open(dp_file) as f:
        dp_results = json.load(f)
    
    # Extract data
    data = []
    for result in dp_results:
        data.append({
            'query': result['query'],
            'epsilon': result['epsilon'],
            'relative_error': result['relative_error'] * 100  # Convert to percentage
        })
    
    df_dp = pd.DataFrame(data)
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for query in df_dp['query'].unique():
        query_data = df_dp[df_dp['query'] == query]
        ax.plot(query_data['epsilon'], query_data['relative_error'], 
                marker='o', linewidth=2, markersize=8, label=query)
    
    ax.set_xlabel('Privacy Parameter (ε)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Relative Error (%)', fontsize=12, fontweight='bold')
    ax.set_title('Privacy-Utility Trade-off: Differential Privacy', fontsize=14, fontweight='bold')
    ax.set_xscale('log')
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'privacy_utility_tradeoff.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'privacy_utility_tradeoff.pdf')
    print(f"  Saved: {PLOTS_DIR / 'privacy_utility_tradeoff.png'}")
    plt.close()

def extract_query_plan_metrics(df_results):
    """
    Extract key metrics from EXPLAIN plans
    - Actual time
    - Rows estimated vs actual
    - Buffer hits/reads
    """
    print("\n>>> Analyzing Query Plans")
    
    plan_metrics = []
    
    for idx, row in df_results.iterrows():
        # plan = row['explain_plan']
        plan = row.get('explain_plan', None)

        # Skip if plan unavailable (dynamic masking case)
        if plan is None:
            continue

        # Some runs may have plan stored as None explicitly
        if not isinstance(plan, dict):
            continue
        
        plan_node = plan.get('Plan', {})
        
        # Extract top-level node metrics
        # actual_time = plan['Actual Total Time']
        # plan_rows = plan.get('Plan Rows', 0)
        # actual_rows = plan.get('Actual Rows', 0)
        actual_time = plan_node.get('Actual Total Time', plan.get('Execution Time', 0))
        plan_rows = plan_node.get('Plan Rows', 0)
        actual_rows = plan_node.get('Actual Rows', 0)
        # Row estimate error
        if actual_rows > 0:
            row_estimate_error = abs(plan_rows - actual_rows) / actual_rows
        else:
            row_estimate_error = 0
        
        # Buffer statistics
        buffers = plan.get('Shared Hit Blocks', 0) + plan.get('Shared Read Blocks', 0)
        
        plan_metrics.append({
            'experiment': row['experiment'],
            'system': row['system'],
            'query_idx': row['query_idx'],
            'actual_time_ms': actual_time,
            'estimated_rows': plan_rows,
            'actual_rows': actual_rows,
            'row_estimate_error': row_estimate_error,
            'buffer_blocks': buffers
        })
    
    df_plans = pd.DataFrame(plan_metrics)
    
    # Summary by system
    summary = df_plans.groupby('system').agg({
        'row_estimate_error': 'mean',
        'buffer_blocks': 'mean'
    }).round(2)
    
    print("\nQuery Plan Summary by System:")
    print(summary)
    
    return df_plans

def main():
    print("=== Analyzing Benchmark Results ===")
    
    # Load summary data
    summary = load_latest_summary()
    
    # Convert results to DataFrame
    df_results = pd.DataFrame(summary['results'])
    
    print(f"\nLoaded {len(df_results)} experiment results")
    print(f"Systems: {df_results['system'].unique()}")
    
    # Generate plots and tables
    plot_latency_comparison(df_results)
    calculate_overhead_table(df_results)
    plot_privacy_utility_tradeoff()
    extract_query_plan_metrics(df_results)
    
    # Storage summary
    if 'storage' in summary:
        print("\n>>> Storage Metrics")
        storage = summary['storage']
        for table, metrics in storage.items():
            print(f"  {table}: {metrics['total_size']}")
    
    print("\n✓ Analysis complete! Check results/plots/")

if __name__ == '__main__':
    main()
# PostgreSQL Anonymizer Benchmarking Project

Comprehensive benchmarking of [postgresql_anonymizer](https://gitlab.com/dalibo/postgresql_anonymizer) extension, comparing performance, storage, and privacy-utility trade-offs against baseline approaches.

## Project Overview

This project evaluates PostgreSQL data anonymization through:

1. **Performance Analysis**: Query latency, throughput, query plan impacts
2. **Storage Overhead**: Disk usage for different masking methods
3. **Privacy Analysis**: k-anonymity, differential privacy simulation, re-identification attacks
4. **Systems Critique**: Index compatibility, integration complexity, planner behavior

### Systems Compared

- **Baseline A**: Native Postgres (no masking)
- **Baseline B**: SQL view-based masking
- **System Under Test**: postgresql_anonymizer extension
  - Dynamic masking (role-based)
  - Static masking (pre-masked tables)

## Requirements

### Hardware
- **CPU**: Multi-core x86_64 (tested on Intel i7-10750H, 6 cores)
- **RAM**: 32GB (minimum 16GB)
- **Storage**: 50GB free space (SSD recommended)

### Software
- **Docker**: 20.10+ and Docker Compose
- **Python**: 3.8+ with pip
- **Datasets**: Adult Census Income, Healthcare (from Kaggle)

### Python Dependencies
```bash
pip install psycopg2-binary pandas numpy faker pyyaml matplotlib seaborn scipy
```

## Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/pdrk2303/Postgres-Data-Anonymizer
cd Postgres-Data-Anonymizer
```

### 2. Download Datasets

Download these datasets and place in `data/raw` directory:

1. **Adult Census Income**  
   https://www.kaggle.com/datasets/uciml/adult-census-income  
   → Save as `data/raw/adult_census.csv`

2. **Healthcare Dataset**  
   https://www.kaggle.com/datasets/prasad22/healthcare-dataset  
   → Save as `data/raw/healthcare_dataset.csv`

### 3. Start PostgreSQL with Anonymizer Extension
```bash
chmod +x ./run_all.sh
./run_all.sh
```

## Repository Structure

```
postgres-anon-benchmark/
├── setup/
│   |── docker-compose.yaml             # PostgreSQL + anonymizer setup
|   |── init_extensions.sql             # Extension initialization
├── sql/
│   ├── create_raw_tables.sql           # Schema definitions
│   ├── create_view_masking.sql         # View-based masking
│   ├── create_anon_rules.sql           # Anonymizer configuration
│   └── masking_functions.sql           # Masking function tests
├── workloads/
│   ├── point_lookup.sql                # Query workloads
│   ├── range_query.sql
│   ├── groupby_aggregate.sql
│   ├── distinct_count.sql
│   └── analytic_topk.sql
├── scripts/
|   ├── setup
|       ├── generate_synthetic.py       # Dataset scaling
│       ├── load_data.py                # Dataset loading
|       └── analyze_results.py          # Results analysis + plotting
|   ├── experiments
│       ├── run_experiments.py          # Main benchmark runner
│       ├── k_anonymity.py              # k-anonymity implementation
│       ├── differential_privacy.py     # DP simulation
│       └── reidentification_attack.py
│   ├── analysis
|       ├── analyze_query_plans.py      # # Main analysis script
│       ├── analyze_results.py          # Calculates masking functions overhead
│       ├── compare_masking_functions.py     
├── data/raw                            # Datasets 
│   ├── adult.csv
│   └── healthcare_dataset.csv
├── results/
│   ├── raw/                            # Raw JSON/CSV results
│   └── plots/                          # Generated figures
├── report/                             
├── experiments_100k.yaml               # Experiment configurations
├── experiments_1M.yaml
├── run_all.sh                          # Script to run all experiments
├── MANIFEST.md                         # Environment details
└── README.md
```

## Troubleshooting

### Docker container won't start
```bash
docker-compose down -v
docker-compose up -d --force-recreate
```

### Extension not loading
```bash
docker exec -it pg-anon-bench psql -U postgres -d benchmark
# In psql:
CREATE EXTENSION anon CASCADE;
SELECT anon.init();
\dx  
```

## References

- [PostgreSQL Anonymizer Docs](https://postgresql-anonymizer.readthedocs.io/)
- [Adult Census Dataset](https://archive.ics.uci.edu/ml/datasets/adult)
- [k-Anonymity Paper](https://dataprivacylab.org/dataprivacy/projects/kanonymity/kanonymity.pdf)
- [Differential Privacy Primer](https://www.cis.upenn.edu/~aaroth/Papers/privacybook.pdf)

# Experimental Environment

## Hardware Specifications

| Component | Specification |
|-----------|---------------|
| **CPU** | Intel Core i7-1165G7 @ 2.80GHz (4 cores, 8 Logical Processors) |
| **RAM** | 32 GB DDR4 |
| **Storage** | 1TB Intel NVMe SSD  (Intel 660p OEM variant, PCIe 3.0×4) |
| **OS** | Ubuntu 24.04 (WSL2 environment) |
| **Kernel** | WSL2 kernel 6.6.87.2-microsoft-standard-WSL2 |

---

## Software Versions

### Database
| Software | Version | Source |
|----------|---------|--------|
| **PostgreSQL** | 16.1 | Docker image: registry.gitlab.com/dalibo/postgresql_anonymizer:latest |
| **postgresql_anonymizer** | 2.0.0 | Included in Docker image |
| **pgcrypto** | 1.3 | PostgreSQL contrib |
| **pg_stat_statements** | 1.11 | PostgreSQL contrib |

### Python Environment
| Package | Version |
|---------|---------|
| **Python** | 3.10.12 |
| **psycopg2-binary** | 2.9.9 |
| **pandas** | 2.1.4 |
| **numpy** | 1.26.2 |
| **faker** | 21.0.0 |
| **pyyaml** | 6.0.1 |
| **matplotlib** | 3.8.2 |
| **seaborn** | 0.13.0 |
| **scipy** | 1.11.4 |

### Container Runtime
| Software | Version |
|----------|---------|
| **Docker** | 28.5.1 |
| **Docker Compose** | 2.40.2 |

---

## PostgreSQL Configuration

**Configuration applied in `docker-compose.yaml`:**
#### Docker Image ID: 157cd1c3405b

```yaml
shared_preload_libraries: 'pg_stat_statements,anon'
pg_stat_statements.track: all
max_parallel_workers_per_gather: 4
work_mem: 256MB
maintenance_work_mem: 512MB
random_page_cost: 1.1 
shared_buffers: 2GB 
```

**Database encoding**: UTF8  
**Locale**: en_US.UTF-8  

---

## Dataset Specifications

### 1. Adult Census Income Dataset

| Attribute | Value |
|-----------|-------|
| **Source** | UCI Machine Learning Repository via Kaggle |
| **URL** | https://www.kaggle.com/datasets/uciml/adult-census-income |
| **Original Rows** | 32,561 |
| **Columns** | 15 |
| **Size (CSV)** | 3.6 MB |
| **Loaded Rows** | 100,000 / 1,000,000 / 5,000,000 (scaled via sampling) |

**Column Types**:
- Numeric: age, fnlwgt, education_num, capital_gain, capital_loss, hours_per_week
- Categorical: workclass, education, marital_status, occupation, relationship, race, sex, native_country, income

### 2. Healthcare Dataset

| Attribute | Value |
|-----------|-------|
| **Source** | Kaggle (synthetic healthcare data) |
| **URL** | https://www.kaggle.com/datasets/prasad22/healthcare-dataset |
| **Original Rows** | 55,501 |
| **Columns** | 15 |
| **Size (CSV)** | 8.5 MB |
| **Loaded Rows** | 100,000 / 1,000,000 / 5,000,000 (scaled via sampling) |

**Column Types**:
- Numeric: age, billing_amount
- Categorical: gender, blood_type, medical_condition, insurance_provider, admission_type, medication, test_results
- Text: name, doctor, hospital, room_number
- Date: date_of_admission, discharge_date

---

## Experimental Parameters

### Benchmark Runs
- **Warmup runs**: 3 (discard)
- **Measured runs**: 5 per experiment
- **Statistical measure**: Median latency (ms)
- **Variability**: Standard deviation reported
- **Cache state**: Warm cache (unless specified as cold-cache experiment)

### Systems Tested
1. **raw**: Native Postgres (Baseline A)
2. **view**: SQL view-based masking (Baseline B)
3. **static**: Static anonymized tables (extension)
4. **dynamic**: Dynamic role-based masking (extension)

### Workload Queries
1. Point lookup (primary key)
2. Range scan with COUNT
3. GROUP BY aggregate
4. Distinct count
5. Top-K with ORDER BY

### Privacy Parameters
- **k-anonymity**: k ∈ {2, 5, 10, 20}
- **Differential Privacy**: ε ∈ {0.1, 0.5, 1.0, 5.0}
- **Age generalization**: 5-year buckets (default)
- **Laplace noise**: Sensitivity calculated per query

### Masking Functions Tested
1. Deterministic hash (HMAC-SHA256)
2. Partial masking (prefix + ***)
3. Shuffle (column permutation)
4. Fake data (anon.fake_*)
5. Noise injection (Laplace-like)
6. Generalization (buckets/categories)

---

## Measurement Methodology

### Timing Collection
- **Wall-clock time**: Python `time.perf_counter()` (client-side)
- **Execution time**: `EXPLAIN (ANALYZE)` actual time (server-side)
- **Planning time**: From EXPLAIN ANALYZE output

### Storage Metrics
```sql
-- Total relation size (table + indexes + TOAST)
SELECT pg_total_relation_size('table_name');

-- Table size only
SELECT pg_relation_size('table_name');

-- Index size
SELECT pg_indexes_size('table_name');
```

### Query Plans
- Collected via `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`
- Parsed for: actual time, estimated vs actual rows, buffer hits/reads
- Saved to `results/raw/*.json` for analysis

### Privacy Metrics
- **k-anonymity**: Group size analysis on quasi-identifiers
- **DP accuracy**: Mean Absolute Error (MAE), Relative Error
- **Re-identification**: Linkage success rate using external QI data
- **Utility**: Cardinality ratio, Jensen-Shannon divergence

---

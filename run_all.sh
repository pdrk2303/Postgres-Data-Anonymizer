#!/bin/bash
set -e  # Exit on error

DATASET_SIZE=${1:-100k}

echo "=================================================="
echo "PostgreSQL Anonymizer Benchmark - Complete Pipeline"
echo "Dataset Size: $DATASET_SIZE"
echo "=================================================="

step 1 "Checking prerequisites"

command -v docker >/dev/null 2>&1 || error "Docker not found. Please install Docker."
command -v docker-compose >/dev/null 2>&1 || error "Docker Compose not found."
command -v python3 >/dev/null 2>&1 || error "Python 3 not found."

success "All prerequisites satisfied"

step 2 "Checking datasets"

if [ ! -f "data/raw/adult_census.csv" ]; then
    error "data/raw/adult.csv not found. Download from: https://www.kaggle.com/datasets/uciml/adult-census-income"
fi

if [ ! -f "data/raw/healthcare_dataset.csv" ]; then
    error "data/raw/healthcare_dataset.csv not found. Download from: https://www.kaggle.com/datasets/prasad22/healthcare-dataset"
fi

success "Datasets found"

step 3 "Installing Python dependencies"

if [ ! -d "venv" ]; then
    python3 -m venv venv
    success "Created virtual environment"
fi

source venv/bin/activate || error "Failed to activate virtual environment"

pip install -q -r requirements.txt
success "Python dependencies installed"

step 4 "Starting PostgreSQL + Anonymizer"

cd docker
docker-compose down -v
docker-compose up -d

echo "Waiting for PostgreSQL to start..."
sleep 10

until docker exec pg-anon-bench pg_isready -U postgres >/dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

cd ..
success "PostgreSQL running"

step 5 "Initializing database"

docker exec -i pg-anon-bench psql -U postgres -d benchmark < setup/init_extensions.sql || error "Extension initialization failed"
success "Extensions initialized"

python scripts/setup/generate_synthetic.py || error "Table creation failed"
success "Tables created"

python scripts/setup/load_data.py|| error "Data loading failed"
success "Data loaded"

step 7 "Setting up masking systems"

docker exec -i pg-anon-bench psql -U postgres -d benchmark < sql/create_view_masking.sql || error "View creation failed"
success "Views created"

docker exec -i pg-anon-bench psql -U postgres -d benchmark < sql/create_anon_rules.sql || error "Anonymizer setup failed"
success "Anonymizer configured"

docker exec -i pg-anon-bench psql -U postgres -d benchmark < sql/masking_functions.sql || error "Masking functions setup failed"
success "Masking functions created"

step 8 "Running benchmark experiments"

case $DATASET_SIZE in
    100k)
        CONFIG="experiments_100k.yaml"
        ;;
    1M)
        CONFIG="experiments_1m.yaml"
        ;;
    *)
        error "Invalid dataset size. Use: 100k, 1m"
        ;;
esac

python scripts/experiments/run_experiments.py --config $CONFIG || error "Experiments failed"
success "Benchmarks complete"

step 9 "Running privacy analyses"

echo "  - k-Anonymity..."
python scripts/experiments/k_anonymity.py --k-values 2 5 10 20 || warning "k-Anonymity failed"

echo "  - Differential Privacy..."
python scripts/experiments/differential_privacy.py --epsilon 0.1 0.5 1.0 5.0 || warning "DP simulation failed"

echo "  - Masking Function Comparison..."
python scripts/analysis/compare_masking_functions.py || warning "Masking comparison failed"

echo "  - Re-identification Attack..."
python scripts/experiments/reidentification_attack.py || warning "Re-identification failed"

success "Privacy analyses complete"

step 10 "Generating plots and analysis"

python scripts/analyze_results.py || error "Analysis failed"
success "Analysis complete"

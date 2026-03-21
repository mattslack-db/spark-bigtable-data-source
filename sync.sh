#!/usr/bin/env bash
# Deploy notebooks and wheel to Databricks using sync.
# Builds the wheel, then runs 'databricks sync'. Uses DATABRICKS_PROFILE from .env.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then set -a; source .env; set +a; fi

echo "Building wheel..."
python -m build --wheel 2>&1

echo "Syncing notebooks and wheel (profile=$DATABRICKS_PROFILE)..."
databricks sync --profile "$DATABRICKS_PROFILE" \
  --exclude src --exclude tests --exclude "*.sh" --exclude ".gitignore" --exclude ".github" \
  --exclude "pyproject.toml" --exclude "pytest.ini" --exclude "pytest-coverage.txt" \
  --exclude "*.md" --exclude "poetry.lock" \
  --include dist --include "notebooks/*.ipynb" --include "gcp*.json" . \
  /Workspace/Users/matt.slack@databricks.com/spark-bigtable-data-source

echo "Done"


#!/usr/bin/env bash
# Authenticate with Databricks. Loads DATABRICKS_HOST and DATABRICKS_PROFILE from .env if present.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then set -a; source .env; set +a; fi

databricks auth login --host "$DATABRICKS_HOST" --profile "$DATABRICKS_PROFILE"

gcloud auth application-default set-quota-project "$GCP_PROJECT_ID"

#!/usr/bin/env bash
# GCP Bigtable instance + table for demo notebooks (Terraform).
#
# Usage:
#   ./scripts/setup-bigtable-demo.sh init      (one-time: download providers)
#   ./scripts/setup-bigtable-demo.sh plan      (preview changes)
#   ./scripts/setup-bigtable-demo.sh apply     (create/update; default)
#   ./scripts/setup-bigtable-demo.sh output   (show project_id, instance_id, env_export, etc.)
#   ./scripts/setup-bigtable-demo.sh destroy  (tear down instance and table)
#
# Loads .env from repo root if present (GCP_PROJECT_ID, BIGTABLE_*). Otherwise use
# terraform/terraform.tfvars or set TF_VAR_project_id (and optional TF_VAR_region, etc.).
# Prerequisites: terraform, gcloud auth application-default login (or GOOGLE_APPLICATION_CREDENTIALS).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$REPO_ROOT/terraform"

# Load .env from repo root if present
if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$REPO_ROOT/.env"
  set +a
fi

# Pass through to Terraform; CLI/env take precedence over .env
export TF_VAR_project_id="${TF_VAR_project_id:-${GCP_PROJECT_ID:-}}"
export TF_VAR_region="${TF_VAR_region:-${BIGTABLE_REGION:-}}"
export TF_VAR_instance_id="${TF_VAR_instance_id:-${BIGTABLE_INSTANCE_ID:-}}"
export TF_VAR_table_id="${TF_VAR_table_id:-${BIGTABLE_TABLE_ID:-}}"

COMMAND="${1:-apply}"

require_project_id() {
  if [[ -z "${TF_VAR_project_id:-}" ]]; then
    if [[ -f "$TF_DIR/terraform.tfvars" ]]; then
      return 0
    fi
    echo "Set GCP_PROJECT_ID or TF_VAR_project_id, or add to .env, or create terraform/terraform.tfvars with project_id"
    exit 1
  fi
}

case "$COMMAND" in
  init)
    terraform -chdir="$TF_DIR" init -input=false
    ;;
  plan)
    require_project_id
    terraform -chdir="$TF_DIR" plan -input=false
    ;;
  apply)
    require_project_id
    terraform -chdir="$TF_DIR" apply -input=false -auto-approve
    echo ""
    echo "--- Set these for notebooks / integration tests ---"
    terraform -chdir="$TF_DIR" output -raw env_export
    echo ""
    ;;
  output)
    terraform -chdir="$TF_DIR" output
    ;;
  destroy)
    require_project_id
    terraform -chdir="$TF_DIR" destroy -auto-approve
    ;;
  *)
    echo "Usage: $0 {init|plan|apply|output|destroy}"
    exit 1
    ;;
esac

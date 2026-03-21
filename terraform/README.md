# Bigtable demo infrastructure (Terraform)

Creates a **DEVELOPMENT** Bigtable instance and a table with **change stream** enabled (7-day retention) and column families `cf1` and `cf2` for the `notebooks/stream-demo` and stateful-processor notebooks.

## Prerequisites

- [Terraform](https://www.terraform.io/downloads) >= 1.0
- GCP project with Bigtable API enabled
- Credentials with **Bigtable Admin** (e.g. `roles/bigtable.admin`):
  - `gcloud auth application-default login`, or
  - `GOOGLE_APPLICATION_CREDENTIALS` pointing to a service account key JSON

Enable the API (if not already):

```bash
gcloud services enable bigtableadmin.googleapis.com --project=YOUR_PROJECT_ID
```

## Quick start

1. **Set your project** (required):

   ```bash
   cd terraform
   export TF_VAR_project_id=your-gcp-project-id
   ```

   Or create `terraform.tfvars`:

   ```hcl
   project_id = "your-gcp-project-id"
   # optional overrides:
   # region         = "us-central1"
   # instance_id    = "bt-demo"
   # table_id       = "changes"
   # column_families = ["cf1", "cf2"]
   ```

2. **Apply**:

   ```bash
   terraform init
   terraform plan   # optional
   terraform apply
   ```

3. **Use in notebooks**  
   After apply, run:

   ```bash
   terraform output -raw env_export
   ```

   Copy the `export` lines into your shell or into a `.env` in the project root so integration tests and notebooks see `GCP_PROJECT_ID`, `BIGTABLE_INSTANCE_ID`, `BIGTABLE_TABLE_ID`, `BIGTABLE_REGION`, and `BIGTABLE_COLUMN_FAMILY`.

   In **notebooks/stream-demo.ipynb** (or Databricks), set the widgets to:
   - **project_id**: same as `GCP_PROJECT_ID`
   - **instance_id**: same as `BIGTABLE_INSTANCE_ID`
   - **table_id**: same as `BIGTABLE_TABLE_ID`
   - **column_family**: `cf1` (or first of `column_families`)
   - **credentials_file**: path to your service account JSON (or leave blank if using `gcp*.json` in cwd)

## Service account for Terraform vs notebooks

- **Terraform** uses Application Default Credentials (or `GOOGLE_APPLICATION_CREDENTIALS`) to create the instance and table. That identity needs `roles/bigtable.admin` (or equivalent) on the project.
- **Notebooks** can use the same key file (e.g. the `gcp*.json` you use locally) or a different SA with at least **Bigtable User** (`roles/bigtable.user`) for read/write and change stream.

## Outputs

| Output            | Description |
|-------------------|-------------|
| `project_id`      | GCP project ID |
| `instance_id`     | Bigtable instance name |
| `table_id`        | Bigtable table name |
| `region`          | GCP region |
| `column_families` | List of column families |
| `env_export`      | Shell snippet to set env vars |

## Destroy

```bash
terraform destroy
```

Deletion protection is off for the instance, so this removes the instance and table.

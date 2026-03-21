output "project_id" {
  description = "GCP project ID for use in notebooks and .env"
  value       = var.project_id
}

output "instance_id" {
  description = "Bigtable instance ID"
  value       = google_bigtable_instance.demo.name
}

output "table_id" {
  description = "Bigtable table ID"
  value       = google_bigtable_table.demo.name
}

output "region" {
  description = "GCP region"
  value       = var.region
}

output "column_families" {
  description = "Column families on the table (first is default for stream-demo)"
  value       = var.column_families
}

# Convenience: env vars to source or copy into .env
output "env_export" {
  description = "Shell snippet to set GCP/Bigtable env vars for integration tests and notebooks"
  value       = <<-EOT
    export GCP_PROJECT_ID="${var.project_id}"
    export BIGTABLE_INSTANCE_ID="${google_bigtable_instance.demo.name}"
    export BIGTABLE_TABLE_ID="${google_bigtable_table.demo.name}"
    export BIGTABLE_REGION="${var.region}"
    export BIGTABLE_COLUMN_FAMILY="${var.column_families[0]}"
  EOT
}

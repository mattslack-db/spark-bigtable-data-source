variable "project_id" {
  description = "GCP project ID for Bigtable"
  type        = string
}

variable "region" {
  description = "GCP region for the Bigtable cluster (e.g. us-central1)"
  type        = string
  default     = "us-central1"
}

variable "instance_id" {
  description = "Bigtable instance ID (used by demo notebooks)"
  type        = string
  default     = "bt-demo"
}

variable "table_id" {
  description = "Bigtable table ID (used by demo notebooks)"
  type        = string
  default     = "changes"
}

variable "column_families" {
  description = "Column family names for the table (cf1 required by stream-demo; add cf2 for stateful processor demo)"
  type        = list(string)
  default     = ["cf1", "cf2"]
}

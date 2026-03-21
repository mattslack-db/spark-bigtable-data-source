# GCP Bigtable for demo notebooks: instance + table with change stream.
# Authenticate via: gcloud auth application-default login
# or set GOOGLE_APPLICATION_CREDENTIALS to a service account key with
# roles/bigtable.admin (or roles/bigtable.user + table admin as needed).

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
}

# PRODUCTION with 1 node is the recommended low-cost option (DEVELOPMENT is deprecated)
resource "google_bigtable_instance" "demo" {
  name                = var.instance_id
  deletion_protection = false

  cluster {
    cluster_id   = "${var.instance_id}-cluster"
    zone         = "${var.region}-a"
    storage_type = "HDD"
    num_nodes    = 1
  }
}

resource "google_bigtable_table" "demo" {
  name          = var.table_id
  instance_name = google_bigtable_instance.demo.name

  # 7-day retention for change stream (required for streaming source demos)
  change_stream_retention = "168h0m0s"

  dynamic "column_family" {
    for_each = toset(var.column_families)
    content {
      family = column_family.value
    }
  }
}

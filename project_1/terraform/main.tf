terraform {
  required_version = ">= 1.5"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

#Tables
#Ref: https://www.youtube.com/watch?v=fBqviV-N-Gw

resource "google_bigquery_table" "rides_green"{
  dataset_id = var.BQ_DATASET
  table_id = "rides_green"
  schema = file("schemas/trips_green.json")
  deletion_protection=false
  depends_on = [
    google_bigquery_dataset.dataset
  ]
}

resource "google_bigquery_table" "rides_yellow"{
  dataset_id = var.BQ_DATASET
  table_id = "rides_yellow"
  schema = file("schemas/trips_yellow.json")
  deletion_protection=false
  depends_on = [
    google_bigquery_dataset.dataset
  ]
}
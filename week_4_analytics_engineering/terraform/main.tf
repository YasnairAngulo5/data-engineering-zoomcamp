terraform {
    required_version = ">=1.3.7"
  backend "local" {} # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Dataset trips_data_all
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "trips_data_all" {
  dataset_id = "trips_data_all"
  project    = var.project
  location   = var.region
}

# Dataset production
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "production" {
  dataset_id = "production"
  project    = var.project
  location   = var.region
}

# Dataset staging
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  project    = var.project
  location   = var.region
}

# Dataset dbt_sandbox
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dbt_yangulo" {
  dataset_id = "dbt_yangulo"
  project    = var.project
  location   = var.region
}

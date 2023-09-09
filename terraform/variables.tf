variable "data_lake_bucket" {
  description = "Name of the data lake in GCS where to store the raw files"
  type        = string
}

variable "project" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET_DEV" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
}

variable "BQ_DATASET_PROD" {
  description = "BigQuery Dataset that dbt will use for during development"
  type        = string
}

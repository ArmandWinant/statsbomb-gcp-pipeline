variable "project_id" {
  default     = "statsbomb-gcp-pipeline"
  description = "Project ID"
}

variable "credentials" {
  default     = "./keys/creds.json"
  description = "Service account credentials file"
}

variable "location" {
  default     = "EU"
  description = "Project location"
}

variable "region" {
  default     = "europe-west3"
  description = "Project region"
}

variable "gcs_storage_class" {
  default     = "STANDARD"
  description = "Bucket storage class"
}

variable "data_bucket" {
  default     = "statsbomb-gcp-pipeline_data"
  description = "Data bucket"
}

variable "code_bucket" {
  default     = "statsbomb-gcp-pipeline_code"
  description = "Code bucket"
}

variable "bq_dataset" {
  default     = "statsbomb-warehouse"
  description = "Data warehouse"
}
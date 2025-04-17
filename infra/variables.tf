variable "project_id" {
  default     = "statsbomb-gcp-pipeline-457108"
  description = "Project ID"
}

variable "credentials" {
  default     = "../.google/terra-airflow.json"
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

variable "zone" {
  default     = "europe-west3-c"
  description = "Project region"
}

variable "gcs_storage_class" {
  default     = "STANDARD"
  description = "Bucket storage class"
}

variable "data_bucket" {
  default     = "statsbomb-gcp-pipeline-457108_data"
  description = "Data bucket"
}

variable "code_bucket" {
  default     = "statsbomb-gcp-pipeline-457108_code"
  description = "Code bucket"
}

variable "bq_dataset" {
  default     = "statsbomb-warehouse"
  description = "Data warehouse"
}

variable "image" {
    description = "Machine Image"
    default = "ubuntu-2004-focal-v20250111"
}

variable "user"{
    description = "User for maching"
    default = "bastienwinant"
}

variable "ssh_key_file" {
  description = "Path to the SSH public key file"
  default     = "~/.ssh/gcp.pub" 

}
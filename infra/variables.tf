variable "project_id" {
  default     = "statsbomb-data-pipeline"
  description = "Project ID"
}

variable "credentials" {
  default     = "../.google/terra-airflow.json"
  description = "Service account creds file path"
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
  description = "Project zone"
}

variable "vpc_name" {
  default = "terraform-network"
}

variable "vm_name" {
  default = "terraform-instance"
}

variable "vm_type" {
  default = "e2-micro"
}

variable "vm_image" {
  default = "debian-cloud/debian-11"
}
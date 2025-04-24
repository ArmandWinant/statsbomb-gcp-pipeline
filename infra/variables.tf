variable "project_id" {
  default = "statsbomb-data-pipeline"
  description = "Project ID"
}

variable "credentials" {
  default = "../.google/terra-airflow.json"
  description = "Service account credentials file"
}

variable "location" {
  default = "EU"
  description = "Project location"
}

variable "region" {
  default = "europe-west3"
  description = "Project region"
}

variable "zone" {
  default = "europe-west3-a"
  description = "Project zone"
}

variable "data_bucket" {
  default = "statsbomb-data-pipeline_data"
  description = "GCS bucket for data files"
}

variable "code_bucket" {
  default = "statsbomb-data-pipeline_code"
  description = "GCS bucket for code files"
}

variable "airflow_vm" {
  default = "airflow-vm"
  description = "VM to run Airflow"
}

variable "airflow_vm_type" {
  default = "e2-micro"
  description = "VM machine type"
}

variable "airflow_vm_image" {
  default = "debian-cloud/debian-11"
  description = "VM image"
}

variable "vpc_network" {
  default = "my-custom-mode-network"
  description = "VPC network"
}

variable "vpc_subnet" {
  default = "my-custom-subnet"
  description = "VPC subnet"
}

variable "vpc_ssh_firewall" {
  default = "allow-ssh"
  description = "VPC firewall"
}
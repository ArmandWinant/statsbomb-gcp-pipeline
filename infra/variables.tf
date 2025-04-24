variable "project_id" {
  default = "statsbomb-data-pipeline"
  description = "Project ID"
  type = string
}

variable "credentials" {
  default = "../.google/terra-airflow.json"
  description = "Service account credentials file"
  type = string
}

variable "location" {
  default = "EU"
  description = "Project location"
  type = string
}

variable "region" {
  default = "europe-west3"
  description = "Project region"
  type = string
}

variable "zone" {
  default = "europe-west3-a"
  description = "Project zone"
  type = string
}

variable "data_bucket" {
  default = "statsbomb-data-pipeline_data"
  description = "GCS bucket for data files"
  type = string
}

variable "code_bucket" {
  default = "statsbomb-data-pipeline_code"
  description = "GCS bucket for code files"
  type = string
}

variable "airflow_vm" {
  default = "airflow-vm"
  description = "VM to run Airflow"
  type = string
}

variable "airflow_vm_type" {
  default = "e2-micro"
  description = "VM machine type"
  type = string
}

variable "airflow_vm_image" {
  default = "debian-cloud/debian-11"
  description = "VM image"
  type = string
}

variable "vpc_network" {
  default = "my-custom-mode-network"
  description = "VPC network"
  type = string
}

variable "vpc_subnet" {
  default = "my-custom-subnet"
  description = "VPC subnet"
  type = string
}

variable "vpc_ssh_firewall" {
  default = "allow-ssh"
  description = "VPC firewall"
  type = string
}
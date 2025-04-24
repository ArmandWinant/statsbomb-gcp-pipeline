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

variable "vm_name" {
  default = "airflow-vm"
  description = "VM to run Airflow"
}

variable "vm_type" {
  default = "e2-micro"
  description = "VM machine type"
}

variable "vm_image" {
  default = "debian-cloud/debian-11"
  description = "VM image"
}

variable "vpc" {
  default = "my-custom-mode-network"
  description = "VPC network"
}

variable "subnet" {
  default = "my-custom-subnet"
  description = "VPC subnet"
}

variable "firewall" {
  default = "allow-ssh"
  description = "VPC firewall"
}
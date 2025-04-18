terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
  credentials = file(var.credentials)
}

resource "google_compute_network" "vpc_network" {
  name = var.vpc_name
}

resource "google_compute_instance" "vm_instance" {
  name         = var.vm_name
  machine_type = var.vm_type
  tags         = ["web", "dev"]

  boot_disk {
    initialize_params {
      image = var.vm_image
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.name
    access_config {
    }
  }
}


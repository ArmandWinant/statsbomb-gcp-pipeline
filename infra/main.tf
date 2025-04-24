terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.31.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials)
}

resource "google_compute_network" "vpc_network" {
  name                    = var.vpc
  auto_create_subnetworks = false
  mtu                     = 1460
}

resource "google_compute_subnetwork" "default" {
  name          = var.subnet
  ip_cidr_range = "10.0.1.0/24"
  region        = var.region
  network       = google_compute_network.vpc_network.id
}

# Create a single Compute Engine instance
resource "google_compute_instance" "default" {
  name         = var.vm_name
  machine_type = var.vm_type
  zone         = var.zone
  tags         = ["ssh"]

  boot_disk {
    initialize_params {
      image = var.vm_image
    }
  }

  # Install Flask
  metadata_startup_script = "sudo apt-get update; sudo apt-get install -yq build-essential python3-pip rsync; pip install flask"

  network_interface {
    subnetwork = google_compute_subnetwork.default.id

    access_config {
      # Include this section to give the VM an external IP address
    }
  }
}

resource "google_compute_firewall" "ssh" {
  name = var.firewall
  allow {
    ports    = ["22"]
    protocol = "tcp"
  }
  direction     = "INGRESS"
  network       = google_compute_network.vpc_network.id
  priority      = 1000
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["ssh"]
}
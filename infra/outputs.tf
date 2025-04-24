output "airflow_vm_public_ip" {
  value = google_compute_instance.airflow_vm.network_interface[0].access_config[0].nat_ip
  description = "Public IP of the VM instance running Airflow"
}
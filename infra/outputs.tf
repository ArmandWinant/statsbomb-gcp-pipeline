output "vm-ip" {
  value       = google_compute_instance.vm_instance.network_interface.0.network_ip
  description = "IP address of the VM instance"
}
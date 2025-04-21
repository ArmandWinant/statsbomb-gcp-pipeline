output "vm-ip" {
  value       = google_compute_instance.vm_instance.network_interface.0.network_ip
  description = "IP address of the VM instance"
}

output "my-email" {
  value = data.google_client_openid_userinfo.me.email
}
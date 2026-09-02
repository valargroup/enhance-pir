output "coordinator_public_ipv4" {
  value = digitalocean_droplet.coordinator.ipv4_address
}

output "coordinator_private_ipv4" {
  value = digitalocean_droplet.coordinator.ipv4_address_private
}

output "worker_public_ipv4" {
  value = [for worker in digitalocean_droplet.worker : worker.ipv4_address]
}

output "worker_private_ipv4" {
  value = [for worker in digitalocean_droplet.worker : worker.ipv4_address_private]
}

output "zakura_volume_id" {
  value = digitalocean_volume.zakura.id
}

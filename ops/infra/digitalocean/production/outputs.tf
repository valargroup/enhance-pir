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

output "worker_groups" {
  description = "Stable shard groups and the two replica Droplets in each group."
  value = [for group in local.worker_groups : {
    name = group.name
    replicas = [for replica_name in group.replicas : {
      name         = replica_name
      public_ipv4  = digitalocean_droplet.worker[index(local.worker_names, replica_name)].ipv4_address
      private_ipv4 = digitalocean_droplet.worker[index(local.worker_names, replica_name)].ipv4_address_private
    }]
  }]
}

output "zakura_volume_id" {
  value = digitalocean_volume.zakura.id
}

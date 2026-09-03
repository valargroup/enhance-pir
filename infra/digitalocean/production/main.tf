locals {
  coordinator_name = "spendability-memo-pir-coordinator-01"
  # One worker serves every table and shard. Append a name here only after
  # reading the note on the one-to-two transition in README.md.
  worker_names = [
    "spendability-memo-pir-worker-01",
  ]
  common_packages = ["ca-certificates", "curl", "jq", "htop"]
}

resource "digitalocean_vpc" "memo" {
  name     = "spendability-memo-pir-poc"
  region   = var.region
  ip_range = "10.142.0.0/24"
}

resource "digitalocean_tag" "coordinator" {
  name = "spendability-memo-pir-coordinator"
}

resource "digitalocean_tag" "worker" {
  name = "spendability-memo-pir-worker"
}

resource "digitalocean_droplet" "coordinator" {
  name       = local.coordinator_name
  image      = var.image
  region     = var.region
  size       = var.size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.memo.id
  tags       = [digitalocean_tag.coordinator.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-coordinator.yaml.tftpl", {
    packages = jsonencode(concat(local.common_packages, ["xfsprogs"]))
  })
}

resource "digitalocean_droplet" "worker" {
  count      = length(local.worker_names)
  name       = local.worker_names[count.index]
  image      = var.image
  region     = var.region
  size       = var.size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.memo.id
  tags       = [digitalocean_tag.worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-worker.yaml.tftpl", {
    packages = jsonencode(local.common_packages)
  })
}

resource "digitalocean_volume" "zakura" {
  region                  = var.region
  name                    = "spendability-memo-pir-zakura"
  size                    = 1024
  initial_filesystem_type = "xfs"
  description             = "Zakura archive and canonical Ironwood memo records for the PIR POC"
}

resource "digitalocean_volume_attachment" "zakura" {
  droplet_id = digitalocean_droplet.coordinator.id
  volume_id  = digitalocean_volume.zakura.id
}

resource "digitalocean_firewall" "coordinator" {
  name = "spendability-memo-pir-coordinator"
  tags = [digitalocean_tag.coordinator.name]

  dynamic "inbound_rule" {
    for_each = var.allowed_ssh_cidrs
    content {
      protocol         = "tcp"
      port_range       = "22"
      source_addresses = [inbound_rule.value]
    }
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8233"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "digitalocean_firewall" "worker" {
  name = "spendability-memo-pir-workers"
  tags = [digitalocean_tag.worker.name]

  dynamic "inbound_rule" {
    for_each = var.allowed_ssh_cidrs
    content {
      protocol         = "tcp"
      port_range       = "22"
      source_addresses = [inbound_rule.value]
    }
  }

  inbound_rule {
    protocol    = "tcp"
    port_range  = "8091"
    source_tags = [digitalocean_tag.coordinator.name]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "digitalocean_project_resources" "memo" {
  project = var.project_id
  resources = concat(
    [digitalocean_droplet.coordinator.urn, digitalocean_volume.zakura.urn],
    [for worker in digitalocean_droplet.worker : worker.urn],
  )
}

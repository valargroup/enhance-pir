locals {
  coordinator_name = "enhance-pir-coordinator-01"
  public_hostname  = "enhance-pir.valargroup.dev"
  # Group order is stable shard placement. Replica membership may change
  # without moving shards; append groups before the next six-shard boundary.
  worker_groups = [
    {
      name = "shard-group-01"
      replicas = [
        "enhance-pir-worker-01",
        "enhance-pir-worker-02",
      ]
    },
  ]
  worker_names    = flatten([for group in local.worker_groups : group.replicas])
  common_packages = ["ca-certificates", "curl", "jq", "htop"]
}

resource "digitalocean_vpc" "enhance" {
  name     = "enhance-pir-production"
  region   = var.region
  ip_range = "10.142.0.0/24"
}

resource "digitalocean_tag" "coordinator" {
  name = "enhance-pir-coordinator"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_tag" "worker" {
  name = "enhance-pir-worker"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_droplet" "coordinator" {
  name       = local.coordinator_name
  image      = var.image
  region     = var.region
  size       = var.coordinator_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.coordinator.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-coordinator.yaml.tftpl", {
    packages = jsonencode(concat(local.common_packages, ["xfsprogs"]))
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_droplet" "worker" {
  count      = length(local.worker_names)
  name       = local.worker_names[count.index]
  image      = var.image
  region     = var.region
  size       = var.worker_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.enhance.id
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
  description             = "Zakura archive and canonical Ironwood Enhance records"

  # DigitalOcean cannot rename a volume in place. Keep the historical provider
  # name so this production data volume is never replaced for branding alone.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [name, description]
  }
}

resource "digitalocean_volume_attachment" "zakura" {
  droplet_id = digitalocean_droplet.coordinator.id
  volume_id  = digitalocean_volume.zakura.id
}

resource "digitalocean_firewall" "coordinator" {
  name = "enhance-pir-coordinator"
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
  name = "enhance-pir-workers"
  tags = [digitalocean_tag.worker.name]

  inbound_rule {
    protocol    = "tcp"
    port_range  = "22"
    source_tags = [digitalocean_tag.coordinator.name]
  }

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

resource "digitalocean_project_resources" "enhance" {
  project = var.project_id
  resources = concat(
    [digitalocean_droplet.coordinator.urn, digitalocean_volume.zakura.urn],
    [for worker in digitalocean_droplet.worker : worker.urn],
  )
}

resource "cloudflare_dns_record" "enhance" {
  zone_id = var.cloudflare_zone_id
  name    = local.public_hostname
  type    = "A"
  content = var.coordinator_dns_ipv4
  ttl     = 300
  proxied = false
  comment = "Enhance PIR production coordinator; managed by Terraform"
}

# Keep the existing production resources while renaming Terraform addresses.
moved {
  from = digitalocean_vpc.memo
  to   = digitalocean_vpc.enhance
}

moved {
  from = digitalocean_project_resources.memo
  to   = digitalocean_project_resources.enhance
}

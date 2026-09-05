variable "digitalocean_token" {
  description = "DigitalOcean API token supplied through TF_VAR_digitalocean_token."
  type        = string
  sensitive   = true
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token with DNS edit access to valargroup.dev, supplied through TF_VAR_cloudflare_api_token."
  type        = string
  sensitive   = true
}

variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID for valargroup.dev."
  type        = string
  default     = "d3ac9657be6101818fed439c62fdcadf"
}

variable "coordinator_dns_ipv4" {
  description = "Stable public IPv4 published for the production coordinator. Update this when the coordinator address changes."
  type        = string
  default     = "167.99.42.60"
}

variable "project_id" {
  description = "Existing enhance-pir DigitalOcean project ID."
  type        = string
  default     = "85639967-fecb-4c8d-88be-c0e3dee3f86c"
}

variable "region" {
  type    = string
  default = "ams3"
}

variable "coordinator_size" {
  type    = string
  default = "m-8vcpu-64gb-intel"
}

variable "worker_size" {
  description = "Worker Droplet size. Four-vCPU workers use the closest AMS3 bundled size; keep process RSS below 2 GiB."
  type        = string
  default     = "s-4vcpu-8gb"
}

variable "image" {
  type    = string
  default = "ubuntu-24-04-x64"
}

variable "ssh_key_ids" {
  description = "Existing public SSH key IDs or fingerprints. Never supply a private key."
  type        = list(string)
}

variable "allowed_ssh_cidrs" {
  description = "Operator CIDRs allowed to SSH to all three hosts."
  type        = list(string)
}

variable "enable_backups" {
  type    = bool
  default = false
}

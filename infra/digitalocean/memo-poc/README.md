# Ironwood memo PIR POC infrastructure

This isolated Terraform state adds three hosts to the existing
`spendability-pir` DigitalOcean project without adopting or modifying
`spendability-pir-01`:

- one `m-8vcpu-64gb-intel` coordinator with a 1 TiB XFS volume at
  `/srv/zakura`;
- two `m-8vcpu-64gb-intel` private PIR workers; and
- a dedicated VPC and firewalls. Only the coordinator may reach worker port
  8091. SSH is restricted to `allowed_ssh_cidrs`.

Supply the API token at runtime. Never commit it or a populated tfvars file:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"; terraform init && terraform plan'
```

Adding capacity is append-only: add another worker URL after provisioning a
host with the same private-worker firewall. The service assigns fixed groups
of two shard IDs per worker, so old shard ownership does not change.

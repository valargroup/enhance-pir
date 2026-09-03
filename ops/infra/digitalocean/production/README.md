# Ironwood PIR production infrastructure

This Terraform root manages the Enhance PIR production fleet in the
`enhance-pir` DigitalOcean project:

- one `m-8vcpu-64gb-intel` coordinator with a 1 TiB XFS volume at
  `/srv/zakura`, running Zakura (archive), ingest, the coordinator, Caddy, and
  the `pir-apm` sidecar;
- one logical shard group with two `s-4vcpu-8gb` private PIR replicas; and
- the unproxied Cloudflare DNS record `enhance-pir.valargroup.dev`; and
- a dedicated VPC and firewalls. Only the coordinator may reach worker port
  8091. SSH is restricted to `allowed_ssh_cidrs`.

That is the whole production shape. Worker pools per table, a second
coordinator, a separate ingest host, and artifact publishing to Spaces are
documented as a growth path in `docs/pir_deployment_architecture.md` §6 and
are deliberately not built.

## State

State is not committed. `.gitignore` excludes `terraform.tfstate*`, `*.tfplan`,
and populated `*.tfvars`. Until the remote backend below is adopted, the state
file lives in the operator's checkout under this directory. When this root was
renamed from `memo-poc`, the untracked state stayed in the old directory of
whichever checkout ran the last apply; move `terraform.tfstate`,
`terraform.tfstate.backup`, and `.terraform/` into `production/` before the
next plan.

Remote state on DigitalOcean Spaces is the intended end state. Copy
`backend.tf.example` to `backend.tf` (it holds no secrets and can be committed
once the bucket exists), create the bucket, and migrate:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export AWS_ACCESS_KEY_ID="$DO_SPACES_KEY" AWS_SECRET_ACCESS_KEY="$DO_SPACES_SECRET"; terraform init -migrate-state'
```

## Plan and apply

Supply the API token at runtime. Never commit it or a populated tfvars file:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG" TF_VAR_cloudflare_api_token="$CF_API_TOKEN"; terraform init && terraform plan'
```

The Cloudflare record remains DNS-only so Caddy can obtain and renew the
origin certificate directly.

Always save and inspect the production plan before applying it. Renaming the
fleet must not replace the coordinator, workers, VPC, volume, or attachment.
The tag resources are the only expected replacements because DigitalOcean tag
names are immutable. The Zakura volume keeps its historical provider name and
has `prevent_destroy` because DigitalOcean cannot rename it in place.

## Capacity

Each ordered shard group owns six shards and has two active-active replicas.
The coordinator sends a query to one ready replica per group and retries its
peer on failure. Publication requires one ready replica in every used group;
the second copy provides redundancy without contributing a duplicate PIR
partial.

Keep group order append-only. Add the next replica pair before the database
crosses a six-shard boundary; adding or replacing a replica within an existing
group does not move shards. Worker process RSS, including retained frontier
generations and rebuild overlap, is enforced with a 2 GiB systemd cgroup limit
and swap disabled. The bundled
`s-4vcpu-8gb` size is the closest currently available AMS3 shape to the desired
4-vCPU/4-GiB worker and leaves additional host memory headroom.

The public origin and dashboard are served at
`https://enhance-pir.valargroup.dev`.

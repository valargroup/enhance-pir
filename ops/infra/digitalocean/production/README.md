# Ironwood PIR production infrastructure

This Terraform root is the production fleet for the ACTION PIR table. It was
the memo POC (`memo-poc`) and the topology is unchanged; only the name and the
scope statement moved. It manages, in the `spendability-pir` DigitalOcean
project:

- one `m-8vcpu-64gb-intel` coordinator with a 1 TiB XFS volume at
  `/srv/zakura`, running Zakura (archive), ingest, the coordinator, Caddy, and
  the `pir-apm` sidecar;
- one `m-8vcpu-64gb-intel` private PIR worker that serves every table and
  shard; and
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
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"; terraform init && terraform plan'
```

## Capacity

Grow the worker host before growing the worker count. If a second worker is
ever needed: provision it with the private-worker firewall and append it to
the inventory the deploy workflow passes to the coordinator (`ENHANCE_WORKERS_JSON`
in the `production` GitHub Environment). The step from one worker to two moves
every shard at or beyond `SHARDS_PER_WORKER` (2) to the new worker and rebuilds
them there once; every append after that moves nothing. Existing entries are
never renamed, reordered, or removed.

`spendability-memo-pir-worker-02` from the proof of concept was removed from
`worker_names`; `terraform apply` destroys it once the fleet has deployed with
the one-entry inventory.

## Legacy host

`spendability-pir-01` (the single-host nullifier/witness server, formerly
managed by `infra/digitalocean/main.tf` and deployed by `deploy.yml`) is
retired. Its Terraform root was removed from the repository; destroying the
droplet is a manual, confirmed operator step once Vizor points only at this
fleet.

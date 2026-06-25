# MicroCloud Charm — Design

Status: **Implemented** (initial version). The `microcloud` charm lives under
`charm/`.

## 1. Goal

Provide a single Juju charm, named **`microcloud`**, that can:

1. **Deploy and operate** a MicroCloud cluster (LXD + optionally MicroCeph and
   MicroOVN) non-interactively across a set of machines, using
   `microcloud preseed`.
2. **Optionally** connect that cluster to observability (COS) — this is *not*
   required to deploy MicroCloud and is only configured when an
   `opentelemetry-collector` subordinate is related via `cos-agent`.

The earlier "integrator" concept (an observability-only charm assuming a
pre-existing cluster) is subsumed into this charm as one of its auto-detected
modes.

## 2. Supported user scenarios

| # | Scenario | How the charm behaves |
|---|----------|-----------------------|
| 1 | MicroCloud deployed **manually** (no charm), then the members are added as Juju machines and the charm is deployed **only for metrics** | The charm auto-detects the initialized cluster (observe-only mode), installs nothing, validates that every member has a Juju unit, and configures metrics **only** once `cos-agent` is related. |
| 2 | Install MicroCloud **via the charm** **and** integrate metrics | Deploy mode bootstraps the cluster via preseed; relating `cos-agent` then enables metrics. |
| 3 | Install MicroCloud **via the charm**, **no metrics** | Deploy mode bootstraps the cluster; without a `cos-agent` relation no metrics work is performed. |
| 4 | Manual MicroCloud, no charm | Charm not involved. |

## 3. Mode auto-detection

Per node, the charm decides its mode at runtime (no config toggle):

- `microcloud.is_snap_installed()` → if the snap is absent → **deploy**.
- `microcloud waitready` then `microcloud status` → exit 0 means the cluster
  is initialized (**observe-only**); non-zero (typically stderr
  `uninitialized`) means **deploy**.

See `charm/src/microcloud.py`.

## 4. Deployment mode

1. Install snaps at configured channels with `--cohort="+"`, then hold
   refreshes (`charm/src/snap.py`). Empty `snap-channel-microceph` /
   `snap-channel-microovn` omit those components (and their preseed sections).
2. Each unit publishes `{hostname, bind-address}` on the `cluster` peer
   relation (`charm/src/cluster.py`).
3. The **leader** is the initiator. Discovery is **unicast**: the preseed sets
   `initiator_address` to the leader's address and lists every system with an
   explicit `address`; no `lookup_subnet` is used.
4. The shared `session_passphrase` is taken from config if set, otherwise the
   leader generates a random one and shares it via a leader-owned Juju secret
   granted to the peer relation.
5. Once every unit has reported identity, the leader renders the preseed
   (`charm/src/preseed.py`) and runs `microcloud preseed`. Joiners run the same
   document and block on the trust session.

## 5. Observe-only mode

- No snaps installed, no bootstrap.
- The leader lists members via `microcloud cluster list --format json` and
  validates them against Juju unit hostnames (`cluster.validate_membership`),
  matched **on hostname**:
  - a MicroCloud member with **no** Juju unit → **BlockedStatus**;
  - a Juju unit whose node is **not** a member → **BlockedStatus**.

## 6. Observability (optional, both modes)

Gated entirely on the `cos-agent` relation:

- On join/change: ensure LXD metrics config (dedicated loopback `:8444`, TLS,
  no client cert), enable the Ceph mgr Prometheus module, install the
  `ovn-exporter` snap, and publish three scrape jobs + dashboards.
- On broken: reset LXD metrics config and (when `cleanup-on-remove=true`)
  remove the ovn-exporter snap.
- The LXD scrape job uses `https` with `tls_config.ca_file` pointing at
  `/var/snap/lxd/common/lxd/cluster.crt` and `insecure_skip_verify: false`.

The MicroCloud cluster itself is **never** destroyed by the charm.

## 7. Relations

- `peers: cluster` (interface `microcloud-peer`) — unit identity + passphrase
  secret coordination.
- `provides: cos-agent` (interface `cos_agent`) — consumed by an
  `opentelemetry-collector` subordinate.

## 8. Config & actions

See `charm/charmcraft.yaml` for the full list. Highlights: snap channels,
session parameters, ceph/storage/OVN preseed options, and observability
tuning. Actions: `bootstrap`, `status`, `force-reconcile`, `dump-config`.

## 9. Layout

```
charm/
  charmcraft.yaml
  src/
    charm.py         # MicroCloudCharm; mode detection, reconcile, observability
    microcloud.py    # microcloud CLI wrappers (status, members, preseed)
    snap.py          # snap install/cohort/hold
    preseed.py       # preseed YAML builder/renderer
    cluster.py       # peer relation + membership validation
    ceph_mgr.py      # Ceph mgr Prometheus module (observability)
    ovn_exporter.py  # ovn-exporter snap (observability)
  terraform/main.tf  # deployment module; observability optional
  tests/unit/test_charm.py
```

## 10. Deployment via Terraform

`charm/terraform/main.tf` deploys the `microcloud` application across
`machine_ids`. Observability is opt-in via `enable_observability` (default
false), which additionally deploys `opentelemetry-collector` and wires it to a
pre-existing COS model.

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

# Terraform module for deploying the MicroCloud charm across a set of
# machines, optionally connecting the cluster to a pre-existing COS Lite model
# via an opentelemetry-collector subordinate.
#
# Usage
# -----
# 1. Add the target machines as manual Juju machines in a model
#    (out of scope for this module; see the Juju docs).
#
# 2. Set the variables below and run:
#      terraform init && terraform apply
#
# 3. To enable observability, set enable_observability = true and pre-create
#    the COS offers:
#      juju offer prometheus:receive-remote-write admin/cos.prometheus
#      juju offer grafana:grafana-dashboard       admin/cos.grafana-dashboards

terraform {
  required_version = ">= 1.5"
  required_providers {
    juju = {
      source  = "juju/juju"
      version = ">= 0.14"
    }
  }
}

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

variable "model_name" {
  description = "Name for the Juju model that will host the MicroCloud units."
  type        = string
  default     = "microcloud"
}

variable "machine_ids" {
  description = <<EOT
List of Juju machine IDs (within model_name) on which to deploy MicroCloud.
These must already exist as manual machines before running terraform apply.
Example: ["0", "1", "2"]
EOT
  type    = list(string)
  default = ["0", "1", "2"]
}

variable "cluster_name" {
  description = "Value for the microcloud-cluster-name charm config label."
  type        = string
  default     = ""
}

variable "microcloud_channel" {
  description = "Charmhub channel for the microcloud charm."
  type        = string
  default     = "latest/stable"
}

# ---- Snap channels (deployment mode) ----

variable "snap_channel_microcloud" {
  description = "Snap channel for the microcloud snap."
  type        = string
  default     = "3/stable"
}

variable "snap_channel_lxd" {
  description = "Snap channel for the lxd snap."
  type        = string
  default     = "6/stable"
}

variable "snap_channel_microceph" {
  description = "Snap channel for the microceph snap (empty to omit MicroCeph)."
  type        = string
  default     = "squid/stable"
}

variable "snap_channel_microovn" {
  description = "Snap channel for the microovn snap (empty to omit MicroOVN)."
  type        = string
  default     = "24.03/stable"
}

variable "session_passphrase" {
  description = "Optional shared session passphrase; auto-generated if empty."
  type        = string
  default     = ""
  sensitive   = true
}

# ---- Observability (optional) ----

variable "enable_observability" {
  description = "If true, deploy opentelemetry-collector and wire it to COS."
  type        = bool
  default     = false
}

variable "otelcol_channel" {
  description = "Charmhub channel for the opentelemetry-collector subordinate."
  type        = string
  default     = "2/stable"
}

variable "cos_model" {
  description = "Name of the pre-existing COS model."
  type        = string
  default     = "cos"
}

variable "scrape_interval" {
  description = "Prometheus scrape interval for the scrape jobs."
  type        = string
  default     = "30s"
}

# ---------------------------------------------------------------------------
# Juju model
# ---------------------------------------------------------------------------

resource "juju_model" "microcloud" {
  name = var.model_name
}

# ---------------------------------------------------------------------------
# Principal: microcloud (one unit per machine)
# ---------------------------------------------------------------------------

resource "juju_application" "microcloud" {
  model = juju_model.microcloud.name
  name  = "microcloud"

  charm {
    name    = "microcloud"
    channel = var.microcloud_channel
  }

  units     = length(var.machine_ids)
  placement = join(",", var.machine_ids)

  config = merge(
    {
      snap-channel-microcloud = var.snap_channel_microcloud
      snap-channel-lxd        = var.snap_channel_lxd
      snap-channel-microceph  = var.snap_channel_microceph
      snap-channel-microovn   = var.snap_channel_microovn
      scrape-interval         = var.scrape_interval
    },
    var.cluster_name != "" ? { microcloud-cluster-name = var.cluster_name } : {},
    var.session_passphrase != "" ? { session-passphrase = var.session_passphrase } : {},
  )
}

# ---------------------------------------------------------------------------
# Optional observability: opentelemetry-collector subordinate + COS relations
# ---------------------------------------------------------------------------

resource "juju_application" "otelcol" {
  count = var.enable_observability ? 1 : 0

  model = juju_model.microcloud.name
  name  = "opentelemetry-collector"

  charm {
    name    = "opentelemetry-collector"
    channel = var.otelcol_channel
  }
}

resource "juju_integration" "cos_agent" {
  count = var.enable_observability ? 1 : 0

  model = juju_model.microcloud.name
  application {
    name     = juju_application.microcloud.name
    endpoint = "cos-agent"
  }
  application {
    name     = juju_application.otelcol[0].name
    endpoint = "cos-agent"
  }
}

resource "juju_integration" "remote_write" {
  count = var.enable_observability ? 1 : 0

  model = juju_model.microcloud.name
  application {
    name     = juju_application.otelcol[0].name
    endpoint = "send-remote-write"
  }
  application {
    offer_url = "admin/${var.cos_model}.prometheus"
  }
}

resource "juju_integration" "grafana_dashboards" {
  count = var.enable_observability ? 1 : 0

  model = juju_model.microcloud.name
  application {
    name     = juju_application.otelcol[0].name
    endpoint = "grafana-dashboards-provider"
  }
  application {
    offer_url = "admin/${var.cos_model}.grafana-dashboards"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "model_name" {
  description = "Juju model hosting the MicroCloud units."
  value       = juju_model.microcloud.name
}

output "microcloud_application" {
  description = "Juju application name for MicroCloud."
  value       = juju_application.microcloud.name
}

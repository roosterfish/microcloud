# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Build MicroCloud preseed YAML for the microcloud charm.

The charm deploys in *unicast* mode: the initiator advertises its address via
``initiator_address`` and every system lists an explicit ``address``. No
``lookup_subnet`` is used (multicast is unreliable on Juju/cloud networks).

The same preseed document is rendered identically on every unit; each
MicroCloud daemon selects its own role by matching its hostname/address
against the ``initiator_address`` field.
"""

from dataclasses import dataclass, field

import yaml


@dataclass
class SystemEntry:
    """A single system participating in the cluster."""

    name: str
    address: str


@dataclass
class PreseedInputs:
    """All inputs required to render a preseed document."""

    initiator_address: str
    session_passphrase: str
    systems: list[SystemEntry]
    session_timeout: int = 3600

    # Ceph
    with_ceph: bool = True
    ceph_cephfs: bool = False
    ceph_public_network: str = ""
    ceph_internal_network: str = ""

    # OVN
    with_ovn: bool = True
    ovn_uplink_interface: str = ""
    ovn_ipv4_gateway: str = ""
    ovn_ipv4_range: str = ""
    ovn_ipv6_gateway: str = ""
    ovn_dns_servers: str = ""
    ovn_underlay_ip: str = ""

    # Storage disk filters
    storage_local_find: str = ""
    storage_ceph_find: str = ""
    storage_wipe: bool = False
    storage_encrypt: bool = False

    extra_ports: dict = field(default_factory=dict)


def build_preseed(inputs: PreseedInputs) -> dict:
    """Return the preseed document as a plain dict."""
    systems: list[dict] = []
    for sys_entry in inputs.systems:
        entry: dict = {"name": sys_entry.name, "address": sys_entry.address}
        if inputs.with_ovn and inputs.ovn_uplink_interface:
            entry["ovn_uplink_interface"] = inputs.ovn_uplink_interface
        if inputs.with_ovn and inputs.ovn_underlay_ip:
            entry["ovn_underlay_ip"] = inputs.ovn_underlay_ip
        systems.append(entry)

    doc: dict = {
        "initiator_address": inputs.initiator_address,
        "session_passphrase": inputs.session_passphrase,
        "session_timeout": inputs.session_timeout,
        "systems": systems,
    }

    # ---- Ceph ----
    if inputs.with_ceph:
        ceph: dict = {}
        if inputs.ceph_cephfs:
            ceph["cephfs"] = True
        if inputs.ceph_public_network:
            ceph["public_network"] = inputs.ceph_public_network
        if inputs.ceph_internal_network:
            ceph["internal_network"] = inputs.ceph_internal_network
        if ceph:
            doc["ceph"] = ceph

    # ---- OVN ----
    if inputs.with_ovn and inputs.ovn_uplink_interface:
        ovn: dict = {}
        if inputs.ovn_ipv4_gateway:
            ovn["ipv4_gateway"] = inputs.ovn_ipv4_gateway
        if inputs.ovn_ipv4_range:
            ovn["ipv4_range"] = inputs.ovn_ipv4_range
        if inputs.ovn_ipv6_gateway:
            ovn["ipv6_gateway"] = inputs.ovn_ipv6_gateway
        if inputs.ovn_dns_servers:
            ovn["dns_servers"] = inputs.ovn_dns_servers
        if ovn:
            doc["ovn"] = ovn

    # ---- Storage disk filters ----
    storage: dict = {}
    if inputs.storage_local_find:
        # "microcloud preseed" defaults an omitted find_min to 1 for local
        # filters, so it can be left unset here.
        storage["local"] = [_disk_filter(inputs.storage_local_find, inputs)]
    if inputs.with_ceph and inputs.storage_ceph_find:
        # Unlike local filters, "microcloud preseed" requires find_min >= 1
        # to be set explicitly for remote (Ceph) filters and errors out
        # ("Remote storage filter cannot be defined with find_min less than
        # 1") if it is omitted, since it refuses to silently assume a
        # minimum for HA storage. Mirror the local filter's implicit
        # default of 1 disk per system here.
        storage["ceph"] = [_disk_filter(inputs.storage_ceph_find, inputs, find_min=1)]
    if storage:
        doc["storage"] = storage

    return doc


def _disk_filter(find: str, inputs: PreseedInputs, find_min: int | None = None) -> dict:
    entry: dict = {"find": find}
    if find_min is not None:
        entry["find_min"] = find_min
    if inputs.storage_wipe:
        entry["wipe"] = True
    if inputs.storage_encrypt:
        entry["encrypt"] = True
    return entry


def render(inputs: PreseedInputs) -> str:
    """Render the preseed document to a YAML string."""
    return yaml.safe_dump(build_preseed(inputs), sort_keys=False, default_flow_style=False)

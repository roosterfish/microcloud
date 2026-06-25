# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for microcloud charm."""

import json

# Stub out the cos_agent library so we don't need the full charm SDK installed
# during unit tests — the library is tested separately.
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call, patch

import pytest

_cos_agent_stub = ModuleType("charms.grafana_agent.v0.cos_agent")


class _FakeCOSAgentProvider:
    def __init__(self, charm, **kwargs):
        self._charm = charm
        self._scrape_configs_fn = kwargs.get("scrape_configs")

    def _on_refresh(self, *_):
        pass


_cos_agent_stub.COSAgentProvider = _FakeCOSAgentProvider
sys.modules.setdefault("charms", ModuleType("charms"))
sys.modules.setdefault("charms.grafana_agent", ModuleType("charms.grafana_agent"))
sys.modules.setdefault("charms.grafana_agent.v0", ModuleType("charms.grafana_agent.v0"))
sys.modules["charms.grafana_agent.v0.cos_agent"] = _cos_agent_stub

# Now import the modules under test.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from ceph_mgr import CephMgrPrometheus
from ovn_exporter import OVNExporter

# ---------------------------------------------------------------------------
# _lxc_config_set helper
# ---------------------------------------------------------------------------


class TestLxcConfigSet:
    def test_calls_lxc_config_set(self):
        from charm import _lxc_config_set

        with patch("charm.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _lxc_config_set("core.metrics_address", "127.0.0.1:8444")
            mock_run.assert_called_once_with(
                ["lxc", "config", "set", "core.metrics_address", "127.0.0.1:8444"],
                capture_output=True,
                text=True,
                check=True,
            )

    def test_raises_on_failure(self):
        import subprocess

        from charm import LXDConfigError, _lxc_config_set

        with patch(
            "charm.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "lxc", stderr="permission denied"),
        ):
            with pytest.raises(LXDConfigError, match="Cannot set LXD config"):
                _lxc_config_set("core.metrics_address", "127.0.0.1:8444")


# ---------------------------------------------------------------------------
# _ensure_lxd_metrics_config
# ---------------------------------------------------------------------------


class TestEnsureLxdMetricsConfig:
    def _make_charm_stub(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        return stub

    def test_sets_metrics_address_and_disables_auth(self):
        from charm import _LXD_METRICS_ADDRESS, MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm._lxc_config_set") as mock_set:
            MicroCloudCharm._ensure_lxd_metrics_config(stub)
            assert mock_set.call_args_list == [
                call("core.metrics_address", _LXD_METRICS_ADDRESS),
                call("core.metrics_authentication", "false"),
            ]

    def test_propagates_lxd_config_error(self):
        from charm import LXDConfigError, MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm._lxc_config_set", side_effect=LXDConfigError("lxd not running")):
            with pytest.raises(LXDConfigError, match="lxd not running"):
                MicroCloudCharm._ensure_lxd_metrics_config(stub)


# ---------------------------------------------------------------------------
# ceph_mgr tests
# ---------------------------------------------------------------------------


class TestCephMgrPrometheus:
    def test_default_port(self):
        ceph = CephMgrPrometheus()
        assert ceph.port == 9283

    def test_custom_port(self):
        ceph = CephMgrPrometheus(port=9284)
        assert ceph.port == 9284

    def test_is_mgr_active_true(self):
        ceph = CephMgrPrometheus()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "Service              Startup  Current  Notes\n"
            "microceph.daemon     enabled  active   -\n"
            "microceph.mgr        enabled  active   -\n"
            "microceph.mon        enabled  active   -\n"
        )
        with patch("subprocess.run", return_value=mock_result):
            assert ceph.is_mgr_active() is True

    def test_is_mgr_active_false_when_not_installed(self):
        ceph = CephMgrPrometheus()
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            assert ceph.is_mgr_active() is False

    def test_is_mgr_active_false_when_inactive(self):
        ceph = CephMgrPrometheus()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "microceph.mgr        enabled  inactive  -\n"
        with patch("subprocess.run", return_value=mock_result):
            assert ceph.is_mgr_active() is False

    def test_ensure_enabled_skips_when_no_mgr(self):
        ceph = CephMgrPrometheus()
        with patch.object(ceph, "is_mgr_active", return_value=False):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                mock_ceph.assert_not_called()

    def test_ensure_enabled_calls_ceph_commands(self):
        ceph = CephMgrPrometheus(port=9283)
        with patch.object(ceph, "is_mgr_active", return_value=True):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                calls = [str(c) for c in mock_ceph.call_args_list]
                assert any("enable" in c for c in calls)
                assert any("server_addr" in c for c in calls)
                assert any("server_port" in c for c in calls)


# ---------------------------------------------------------------------------
# ovn_exporter tests
# ---------------------------------------------------------------------------


class TestOVNExporter:
    def test_default_channel(self):
        ovn = OVNExporter()
        assert ovn.channel == "latest/edge"

    def test_custom_channel(self):
        ovn = OVNExporter(channel="1/stable")
        assert ovn.channel == "1/stable"

    def test_is_healthy_not_installed(self):
        ovn = OVNExporter()
        with patch.object(ovn, "_is_installed", return_value=False):
            ok, reason = ovn.is_healthy()
            assert ok is False
            assert "not installed" in reason

    def test_is_healthy_missing_connection(self):
        ovn = OVNExporter()
        with (
            patch.object(ovn, "_is_installed", return_value=True),
            patch.object(
                ovn,
                "_missing_connections",
                return_value=[("ovn-exporter:ovn-chassis", "microovn:ovn-chassis")],
            ),
            patch.object(ovn, "_is_service_active", return_value=True),
        ):
            ok, reason = ovn.is_healthy()
            assert ok is False
            assert "Missing snap connections" in reason

    def test_is_healthy_service_inactive(self):
        ovn = OVNExporter()
        with (
            patch.object(ovn, "_is_installed", return_value=True),
            patch.object(ovn, "_missing_connections", return_value=[]),
            patch.object(ovn, "_is_service_active", return_value=False),
        ):
            ok, reason = ovn.is_healthy()
            assert ok is False
            assert "not active" in reason

    def test_is_healthy_all_ok(self):
        ovn = OVNExporter()
        with (
            patch.object(ovn, "_is_installed", return_value=True),
            patch.object(ovn, "_missing_connections", return_value=[]),
            patch.object(ovn, "_is_service_active", return_value=True),
        ):
            ok, reason = ovn.is_healthy()
            assert ok is True
            assert reason == ""

    def test_remove_noop_when_not_installed(self):
        ovn = OVNExporter()
        with patch.object(ovn, "_is_installed", return_value=False):
            with patch("ovn_exporter._run") as mock_run:
                ovn.remove()
                mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# Scrape config generation (via charm._build_scrape_configs)
# ---------------------------------------------------------------------------


class TestScrapeConfigs:
    """Test the scrape config generation logic without a full Harness."""

    def _make_charm_stub(self, config: dict, unit_name: str = "microcloud/0"):
        """Create a minimal stub that exercises _build_scrape_configs."""
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._ceph = None
        stub._ovn = None
        stub.config = config
        stub.app.name = "microcloud"
        stub.unit.name = unit_name
        stub._build_scrape_configs = lambda: MicroCloudCharm._build_scrape_configs(stub)
        stub._cluster_label = lambda: MicroCloudCharm._cluster_label(stub)
        stub._member_label = lambda: MicroCloudCharm._member_label(stub)
        return stub

    def test_all_services_enabled(self, tmp_path):
        stub = self._make_charm_stub(
            {
                "enable-lxd": True,
                "enable-microceph": True,
                "enable-microovn": True,
                "microcloud-cluster-name": "test-cluster",
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )

        stub._ceph = CephMgrPrometheus(port=9283)
        stub._ovn = OVNExporter()

        with (
            patch.object(stub._ceph, "is_mgr_active", return_value=True),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            from charm import MicroCloudCharm

            configs = MicroCloudCharm._build_scrape_configs(stub)

        job_names = [c["job_name"] for c in configs]
        assert "microcloud-lxd" in job_names
        assert "microcloud-microceph" in job_names
        assert "microcloud-microovn" in job_names

    def test_lxd_job_uses_https_with_ca_file(self):
        """LXD scrape job must use https scheme and trust the cluster cert via ca_file.

        core.metrics_authentication=false removes the need for a client cert,
        but the metrics endpoint always speaks TLS — the scrape job must use
        https and supply ca_file so the collector can verify the self-signed
        LXD cluster certificate.
        """
        from charm import _LXD_METRICS_ADDRESS, MicroCloudCharm

        stub = self._make_charm_stub(
            {
                "enable-lxd": True,
                "enable-microceph": False,
                "enable-microovn": False,
                "microcloud-cluster-name": "c",
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        with patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")):
            configs = MicroCloudCharm._build_scrape_configs(stub)

        lxd_job = next(c for c in configs if c["job_name"] == "microcloud-lxd")
        assert lxd_job["scheme"] == "https", "LXD scrape job must use https"
        assert lxd_job["static_configs"][0]["targets"] == [_LXD_METRICS_ADDRESS]

        tls = lxd_job.get("tls_config", {})
        assert tls, "LXD scrape job must have tls_config"
        assert "cert_file" not in tls, "No client cert required — metrics_authentication=false"
        assert "key_file" not in tls, "No client key required — metrics_authentication=false"

    def test_lxd_disabled(self):
        stub = self._make_charm_stub(
            {
                "enable-lxd": False,
                "enable-microceph": False,
                "enable-microovn": False,
                "microcloud-cluster-name": "test-cluster",
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        with patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")):
            from charm import MicroCloudCharm

            configs = MicroCloudCharm._build_scrape_configs(stub)

        assert configs == []

    def test_cluster_label_uses_config(self):
        stub = self._make_charm_stub(
            {
                "microcloud-cluster-name": "my-cluster",
                "enable-lxd": False,
                "enable-microceph": False,
                "enable-microovn": False,
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        from charm import MicroCloudCharm

        assert MicroCloudCharm._cluster_label(stub) == "my-cluster"

    def test_cluster_label_falls_back_to_app_name(self):
        stub = self._make_charm_stub(
            {
                "microcloud-cluster-name": "",
                "enable-lxd": False,
                "enable-microceph": False,
                "enable-microovn": False,
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        from charm import MicroCloudCharm

        assert MicroCloudCharm._cluster_label(stub) == "microcloud"


# ---------------------------------------------------------------------------
# preseed YAML generation
# ---------------------------------------------------------------------------


class TestPreseed:
    def _inputs(self, **overrides):
        from preseed import PreseedInputs, SystemEntry

        base = {
            "initiator_address": "10.0.0.1",
            "session_passphrase": "secret",
            "systems": [
                SystemEntry(name="node1", address="10.0.0.1"),
                SystemEntry(name="node2", address="10.0.0.2"),
            ],
        }
        base.update(overrides)
        return PreseedInputs(**base)

    def test_unicast_systems_have_addresses(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs())
        assert doc["initiator_address"] == "10.0.0.1"
        assert "lookup_subnet" not in doc
        assert doc["systems"][0] == {"name": "node1", "address": "10.0.0.1"}
        assert doc["systems"][1]["address"] == "10.0.0.2"

    def test_passphrase_present(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs())
        assert doc["session_passphrase"] == "secret"

    def test_ceph_section_included(self):
        from preseed import build_preseed

        doc = build_preseed(
            self._inputs(with_ceph=True, ceph_cephfs=True, ceph_public_network="10.0.0.0/24")
        )
        assert doc["ceph"]["cephfs"] is True
        assert doc["ceph"]["public_network"] == "10.0.0.0/24"

    def test_ceph_omitted_when_disabled(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs(with_ceph=False, ceph_cephfs=True))
        assert "ceph" not in doc

    def test_ovn_uplink_section(self):
        from preseed import build_preseed

        doc = build_preseed(
            self._inputs(
                with_ovn=True, ovn_uplink_interface="eth1", ovn_ipv4_gateway="192.0.2.1/24"
            )
        )
        assert doc["ovn"]["ipv4_gateway"] == "192.0.2.1/24"
        for sysentry in doc["systems"]:
            assert sysentry["ovn_uplink_interface"] == "eth1"

    def test_ovn_omitted_without_interface(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs(with_ovn=True, ovn_uplink_interface=""))
        assert "ovn" not in doc
        assert "ovn_uplink_interface" not in doc["systems"][0]

    def test_storage_filters(self):
        from preseed import build_preseed

        doc = build_preseed(
            self._inputs(
                storage_local_find="size > 10GiB",
                storage_ceph_find="type == nvme",
                storage_wipe=True,
            )
        )
        assert doc["storage"]["local"][0] == {"find": "size > 10GiB", "wipe": True}
        assert doc["storage"]["ceph"][0] == {"find": "type == nvme", "find_min": 1, "wipe": True}

    def test_render_produces_yaml(self):
        import yaml

        from preseed import render

        text = render(self._inputs())
        parsed = yaml.safe_load(text)
        assert parsed["initiator_address"] == "10.0.0.1"


# ---------------------------------------------------------------------------
# snap installation helpers
# ---------------------------------------------------------------------------


class TestSnap:
    def test_ensure_snaps_skips_empty_channel(self):
        import snap

        with patch("snap.install") as mock_install, patch("snap.hold") as mock_hold:
            snap.ensure_snaps({"lxd": "6/stable", "microceph": "", "microcloud": "3/stable"})
            installed = [c.args[0] for c in mock_install.call_args_list]
            assert "microceph" not in installed
            assert "lxd" in installed
            assert "microcloud" in installed
            assert mock_hold.call_count == 2

    def test_install_uses_cohort(self):
        import snap

        with patch("snap.is_installed", return_value=False):
            with patch("snap._run") as mock_run:
                snap.install("lxd", "6/stable")
                mock_run.assert_called_once_with(
                    ["snap", "install", "lxd", "--channel", "6/stable", "--cohort=+"]
                )

    def test_install_refreshes_when_present(self):
        import snap

        with patch("snap.is_installed", return_value=True):
            with patch("snap._run") as mock_run:
                snap.install("lxd", "6/stable")
                mock_run.assert_called_once_with(
                    ["snap", "refresh", "lxd", "--channel", "6/stable", "--cohort=+"]
                )


# ---------------------------------------------------------------------------
# membership validation
# ---------------------------------------------------------------------------


class TestMembershipValidation:
    def test_consistent(self):
        from cluster import validate_membership

        problems = validate_membership({"node1", "node2"}, {"node1", "node2"})
        assert problems == []

    def test_member_without_juju_unit_blocks(self):
        from cluster import validate_membership

        problems = validate_membership({"node1"}, {"node1", "node2"})
        assert any("node2" in p and "not deployed as a Juju unit" in p for p in problems)

    def test_juju_unit_not_a_member_blocks(self):
        from cluster import validate_membership

        problems = validate_membership({"node1", "node3"}, {"node1"})
        assert any("node3" in p and "not a MicroCloud member" in p for p in problems)


# ---------------------------------------------------------------------------
# microcloud CLI wrappers
# ---------------------------------------------------------------------------


class TestMicroCloudWrappers:
    def test_is_initialized_false_when_snap_absent(self):
        import microcloud

        with patch("microcloud.is_snap_installed", return_value=False):
            assert microcloud.is_initialized() is False

    def test_is_initialized_true_on_exit_zero(self):
        import microcloud

        with patch("microcloud.is_snap_installed", return_value=True):
            with patch("microcloud.subprocess.run", return_value=MagicMock(returncode=0)):
                assert microcloud.is_initialized() is True

    def test_is_initialized_false_on_nonzero(self):
        import microcloud

        with patch("microcloud.is_snap_installed", return_value=True):
            with patch(
                "microcloud.subprocess.run",
                return_value=MagicMock(returncode=1, stderr="uninitialized"),
            ):
                assert microcloud.is_initialized() is False

    def test_list_members_parses_json(self):
        import microcloud

        payload = json.dumps(
            [
                {"name": "node1", "address": "10.0.0.1:9443", "status": "ONLINE"},
                {"name": "node2", "address": "10.0.0.2:9443", "status": "ONLINE"},
            ]
        )
        with patch(
            "microcloud.subprocess.run", return_value=MagicMock(returncode=0, stdout=payload)
        ):
            members = microcloud.list_members()
        assert [m.name for m in members] == ["node1", "node2"]
        assert members[0].address == "10.0.0.1:9443"

    def test_list_members_raises_on_error(self):
        import microcloud

        with patch(
            "microcloud.subprocess.run", return_value=MagicMock(returncode=1, stderr="boom")
        ):
            with pytest.raises(microcloud.MicroCloudError):
                microcloud.list_members()

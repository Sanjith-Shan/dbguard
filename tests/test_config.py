from pathlib import Path

import pytest
from pydantic import ValidationError

from dbguard.config import FleetConfig, load_config

REPO = Path(__file__).resolve().parents[1]


def test_repo_fleet_yaml_loads():
    cfg = load_config(REPO / "deploy" / "fleet.yaml", env={})
    assert cfg.mode == "dbguard"
    assert cfg.sets["rs1"].nodes == ["mysql-a1", "mysql-a2", "mysql-a3"]
    assert cfg.sets["rs2"].spare == "mysql-b4"
    assert cfg.mysql.repl_user == "repl" and cfg.mysql.port == 3306
    assert cfg.agent_port == 8080 and cfg.cooldown_s == 20 and cfg.rejoin == "auto"
    assert cfg.set_of("mysql-b4") == "rs2" and cfg.set_of("nope") is None


def test_env_overrides_mode():
    cfg = load_config(REPO / "deploy" / "fleet.yaml", env={"DBGUARD_MODE": "naive"})
    assert cfg.mode == "naive"


def test_bad_mode_rejected():
    with pytest.raises(ValidationError):
        load_config(REPO / "deploy" / "fleet.yaml", env={"DBGUARD_MODE": "yolo"})


def test_defaults_and_minimal(tmp_path):
    p = tmp_path / "f.yaml"
    p.write_text("sets:\n  rs1: {nodes: [n1, n2]}\n")
    cfg = load_config(p, env={})
    assert cfg.detect_window_s == 5 and cfg.mysql.user == "dbguard"
    assert cfg.sets["rs1"].spare is None


@pytest.mark.parametrize("sets", [
    {"rs1": {"nodes": ["a", "a"]}},
    {"rs1": {"nodes": ["a"], "spare": "a"}},
    {"rs1": {"nodes": ["a"]}, "rs2": {"nodes": ["a"]}},
    {"rs1": {"nodes": []}},
])
def test_invalid_sets(sets):
    with pytest.raises(ValidationError):
        FleetConfig.model_validate({"sets": sets})


def test_unknown_key_rejected():
    with pytest.raises(ValidationError):
        FleetConfig.model_validate({"sets": {"rs1": {"nodes": ["a"]}}, "detect_windw_s": 3})

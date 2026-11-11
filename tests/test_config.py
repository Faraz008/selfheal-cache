import pytest

from node.config import Member, NodeConfig, parse_size


def test_parse_size():
    assert parse_size("64mb") == 64 << 20
    assert parse_size("1.5k") == 1536
    assert parse_size("500") == 500
    with pytest.raises(SystemExit):
        parse_size("lots")


def test_member_parse():
    assert Member.parse("n1@127.0.0.1:7001:8001") == Member("n1", "127.0.0.1", 7001, 8001)


def test_member_parse_bad():
    with pytest.raises(SystemExit):
        Member.parse("garbage")


def test_validate_self_must_be_member():
    with pytest.raises(SystemExit):
        NodeConfig(node_id="n9", members=["n1@h:1:2"]).validate()


def test_validate_write_acks_range():
    with pytest.raises(SystemExit):
        NodeConfig(replicas=2, write_acks=3).validate()


def test_quorum_mode_requires_overlapping_quorums():
    with pytest.raises(SystemExit, match="overlap"):
        NodeConfig(consistency="quorum", replicas=3, write_acks=1, read_quorum=2).validate()
    NodeConfig(consistency="quorum", replicas=3, write_acks=2, read_quorum=2).validate()
    NodeConfig(consistency="eventual", replicas=3, write_acks=1).validate()  # no constraint


def test_unknown_modes_are_rejected():
    with pytest.raises(SystemExit):
        NodeConfig(consistency="strong").validate()
    with pytest.raises(SystemExit):
        NodeConfig(eviction_policy="random").validate()

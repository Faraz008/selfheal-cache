import pytest

from node.config import Member, NodeConfig


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

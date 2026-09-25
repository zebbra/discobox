"""NetboxClient.upsert_cable: an existing cable is checked against the wanted link."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient  # noqa: E402


class _Cable:
    def __init__(self, id_, a, b, owner=""):
        self.id = id_
        self.a_terminations = [SimpleNamespace(id=i) for i in a]
        self.b_terminations = [SimpleNamespace(id=i) for i in b]
        self.custom_fields = {"source": owner or None}
        self.deleted = False
        self.updates: list = []

    def delete(self):
        self.deleted = True

    def update(self, patch):
        self.updates.append(patch)


class _FakeNB:
    def __init__(self, cables: list[_Cable], ifaces: dict[int, int | None]):
        self.cables_by_id = {c.id: c for c in cables}
        self.created: list = []
        ifs = {i: SimpleNamespace(id=i, cable=SimpleNamespace(id=c) if c else None) for i, c in ifaces.items()}

        def _create(**kw):
            self.created.append(kw)
            return _Cable(99, [kw["a_terminations"][0]["object_id"]], [kw["b_terminations"][0]["object_id"]])

        self.nb = SimpleNamespace(dcim=SimpleNamespace(
            interfaces=SimpleNamespace(get=lambda i: ifs.get(i)),
            cables=SimpleNamespace(get=lambda i: self.cables_by_id.get(i), create=_create),
        ))


def _upsert(fake: _FakeNB) -> str:
    client = NetboxClient.__new__(NetboxClient)
    client.nb = fake.nb
    return client.upsert_cable(1, 2, source_cf="source", source_value="netdisco")


def test_no_cable_creates() -> None:
    fake = _FakeNB([], {1: None, 2: None})
    assert _upsert(fake) == "created" and len(fake.created) == 1


def test_correct_cable_exists_and_unowned_is_claimed() -> None:
    cable = _Cable(7, [1], [2])
    fake = _FakeNB([cable], {1: 7, 2: 7})
    assert _upsert(fake) == "exists"
    assert cable.updates == [{"custom_fields": {"source": "netdisco"}}] and not fake.created


def test_dangling_cable_is_replaced() -> None:
    # peer device deleted: Netbox kept the cable with only the remote end
    cable = _Cable(7, [2], [])
    fake = _FakeNB([cable], {1: None, 2: 7})
    assert _upsert(fake) == "created"
    assert cable.deleted and len(fake.created) == 1


def test_own_cable_to_other_port_is_replaced() -> None:
    cable = _Cable(7, [1], [3], owner="netdisco")      # re-patched: 1 now goes to 2
    fake = _FakeNB([cable], {1: 7, 2: None})
    assert _upsert(fake) == "created" and cable.deleted


def test_manual_cable_to_other_port_is_a_conflict() -> None:
    cable = _Cable(7, [1], [3])                         # unowned, leads elsewhere
    fake = _FakeNB([cable], {1: 7, 2: None})
    assert _upsert(fake) == "conflict"
    assert not cable.deleted and not fake.created and not cable.updates


def test_foreign_owned_correct_cable_is_a_conflict() -> None:
    cable = _Cable(7, [1], [2], owner="bossy")
    fake = _FakeNB([cable], {1: 7, 2: 7})
    assert _upsert(fake) == "conflict" and not cable.deleted

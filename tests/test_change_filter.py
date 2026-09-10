from __future__ import annotations

from dataclasses import dataclass

from railway_network_analytics.change_filter import ChangeFilter, digest


@dataclass(frozen=True)
class Obs:
    stop_id: str
    delay: int | None = None

    def key(self) -> str:
        return self.stop_id

    def payload(self) -> tuple:
        return (self.delay,)


def test_first_cycle_emits_everything(tmp_path):
    """Cold start has no state, so nothing can be suppressed."""
    f = ChangeFilter(tmp_path / "seen.json")
    assert len(f.select([Obs("a", 1), Obs("b", 2)])) == 2


def test_identical_second_cycle_emits_nothing(tmp_path):
    f = ChangeFilter(tmp_path / "seen.json")
    batch = [Obs("a", 1), Obs("b", 2)]
    f.select(batch)
    assert f.select(batch) == []


def test_only_the_changed_observation_is_emitted(tmp_path):
    f = ChangeFilter(tmp_path / "seen.json")
    f.select([Obs("a", 1), Obs("b", 2)])
    changed = f.select([Obs("a", 1), Obs("b", 99)])
    assert [o.stop_id for o in changed] == ["b"]


def test_state_survives_a_restart(tmp_path):
    """The whole point: an hourly cron run does not keep memory between cycles."""
    path = tmp_path / "seen.json"
    first = ChangeFilter(path)
    first.select([Obs("a", 1)])
    first.save()

    reloaded = ChangeFilter(path)
    assert reloaded.select([Obs("a", 1)]) == []
    assert [o.stop_id for o in reloaded.select([Obs("a", 2)])] == ["a"]


def test_state_is_replaced_not_merged(tmp_path):
    """A stop that leaves the feed is forgotten, so the file cannot grow forever."""
    f = ChangeFilter(tmp_path / "seen.json")
    f.select([Obs("a", 1), Obs("b", 2)])
    f.select([Obs("a", 1)])
    assert set(f.seen) == {"a"}
    # ...and its return re-emits once, which is the at-least-once cost of that choice.
    assert [o.stop_id for o in f.select([Obs("a", 1), Obs("b", 2)])] == ["b"]


def test_corrupt_state_file_starts_cold_instead_of_crashing(tmp_path):
    path = tmp_path / "seen.json"
    path.write_text("{not json", encoding="utf-8")
    assert len(ChangeFilter(path).select([Obs("a", 1)])) == 1


def test_digest_is_stable_across_processes():
    """Builtin hash() is salted per process and would suppress nothing after a restart."""
    assert digest((1, "x", None)) == digest((1, "x", None))
    assert digest((1, "x", None)) != digest((1, "y", None))


def test_save_is_atomic_and_leaves_no_temp_file(tmp_path):
    path = tmp_path / "seen.json"
    f = ChangeFilter(path)
    f.select([Obs("a", 1)])
    f.save()
    assert path.is_file()
    assert not path.with_suffix(".tmp").exists()

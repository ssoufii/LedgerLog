"""Tests for the deliberate damage the demo stages, story M11.7.

Two kinds of claim are checked here.

The first is that each scenario does what it says and that recovery answers it
the way ARCHITECTURE.md section 6 says it should: a torn log loses its torn
record and nothing before it, a flipped byte loses the record it landed in, a
table without a usable footer is passed over while its records come back from
the log, a killed merge leaves its sources intact, and an expired record reads
as absent. Those are the same claims the engine's own crash tests make; what is
new here is that one module stages them, so the demonstration on the web page
and the assertion in the test suite are the same code rather than two
descriptions of it.

The second is that running a scenario cannot hurt the database it was pointed
at. A visitor pulls the plug on a database they have been typing into, so every
test that damages one also checks the original is byte for byte what it was.

Finally, the tiles on the page name a pytest for each scenario, and a test here
checks that every name they give is a test that exists. A tile claiming to be
covered by a test nobody wrote would be the exact invented proof the design
spec rules out.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from ledgerlog import faults
from ledgerlog.engine import LedgerLog

REPO_ROOT = Path(__file__).resolve().parent.parent
TILE_DATA = REPO_ROOT / "web" / "src" / "data" / "crash-scenarios.json"


@pytest.fixture
def written(tmp_path: Path) -> Iterator[Path]:
    """A database with three records in it, closed, the way a visitor leaves one."""
    directory = tmp_path / "data"
    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as engine:
        for index in range(3):
            engine.put(f"key-{index}".encode(), f"value-{index}".encode())
    yield directory


def fingerprint(directory: Path) -> dict[str, str]:
    """Every file in ``directory``, by name and by digest."""
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.iterdir())
        if path.is_file()
    }


# ---------------------------------------------------------------------------
# The visitor's own database is never the one that gets damaged
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scenario", faults.SCENARIOS)
def test_a_scenario_leaves_the_original_database_untouched(written: Path, scenario: str) -> None:
    before = fingerprint(written)

    faults.run_scenario(written, scenario)

    assert fingerprint(written) == before


@pytest.mark.parametrize("scenario", faults.SCENARIOS)
def test_a_scenario_leaves_no_temporary_directory_behind(written: Path, scenario: str) -> None:
    import tempfile

    root = Path(tempfile.gettempdir())
    before = {path.name for path in root.glob("ledgerlog-crash-*")}

    faults.run_scenario(written, scenario)

    assert {path.name for path in root.glob("ledgerlog-crash-*")} == before


@pytest.mark.parametrize("scenario", faults.SCENARIOS)
def test_a_scenario_reports_plain_json(written: Path, scenario: str) -> None:
    report = faults.run_scenario(written, scenario)

    assert json.loads(json.dumps(report)) == report
    assert report["scenario"] == scenario
    assert report["damage"]


def test_an_unknown_scenario_is_refused(written: Path) -> None:
    with pytest.raises(ValueError, match="unknown scenario"):
        faults.run_scenario(written, "set-fire-to-it")


def test_a_scenario_with_nothing_to_damage_says_so(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(directory, fsync_policy="never", flush_in_background=False):
        pass

    with pytest.raises(faults.ScenarioNotPossible):
        faults.run_scenario(directory, "torn-write")


# ---------------------------------------------------------------------------
# What recovery makes of each accident
# ---------------------------------------------------------------------------


def test_a_torn_log_loses_its_torn_record_and_nothing_before_it(written: Path) -> None:
    report = faults.run_scenario(written, "torn-write")

    assert report["keys_before"] == ["key-0", "key-1", "key-2"]
    assert report["keys_after"] == ["key-0", "key-1"]
    assert report["dropped"] == ["key-2"]
    # The engine says where it stopped, and that is the whole of why.
    assert report["recovery"]["stopped_at"] is not None
    assert report["recovery"]["reason"]
    assert report["recovery"]["records_replayed"] == 2


def test_a_flipped_byte_loses_the_record_it_landed_in(written: Path) -> None:
    report = faults.run_scenario(written, "flipped-byte")

    assert report["dropped"] == ["key-2"]
    assert report["recovery"]["records_replayed"] == 2
    assert "checksum" in (report["recovery"]["reason"] or "")


def test_a_flush_killed_before_its_footer_loses_nothing(written: Path) -> None:
    report = faults.run_scenario(written, "crash-mid-flush")

    assert report["dropped"] == []
    assert report["keys_after"] == report["keys_before"]
    # The file is still on disk, rejected rather than read, and the engine says
    # which one and why.
    assert [rejection["status"] for rejection in report["tables_rejected"]] == ["INCOMPLETE"]
    assert "footer" in (report["tables_rejected"][0]["reason"] or "")


def test_a_footer_that_no_longer_verifies_is_not_trusted(written: Path) -> None:
    report = faults.run_scenario(written, "missing-footer")

    assert report["dropped"] == []
    assert [rejection["status"] for rejection in report["tables_rejected"]] == ["INCOMPLETE"]
    assert "checksum" in (report["tables_rejected"][0]["reason"] or "")


def test_a_merge_killed_before_its_footer_keeps_every_source(written: Path) -> None:
    report = faults.run_scenario(written, "crash-mid-compaction")

    assert report["dropped"] == []
    assert report["keys_after"] == report["keys_before"]
    # Both sources are loaded again, and the partial output is not among them.
    assert len(report["tables_loaded"]) == report["tables_staged"] >= 2
    assert all(name.endswith(".sst") for name in report["tables_loaded"])


def test_an_expired_record_reads_as_absent_after_a_restart(written: Path) -> None:
    report = faults.run_scenario(written, "expired-record")

    assert report["dropped"] == ["key-0"]
    assert "key-0" not in report["keys_after"]
    # Nothing was damaged, so nothing was rejected: the record is gone because
    # its deadline passed, not because the file was broken.
    assert report["tables_rejected"] == []
    assert report["recovery"]["reason"] is None


def test_live_keys_asks_the_engine_rather_than_the_files(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as engine:
        engine.put(b"kept", b"value")
        engine.put(b"deleted", b"value")
        engine.delete(b"deleted")
        engine.flush()

        # Both keys are records in the table; only one is a key the engine
        # answers for.
        assert faults.live_keys(engine) == ["kept"]


# ---------------------------------------------------------------------------
# The tiles on the page name tests that exist
# ---------------------------------------------------------------------------


def tile_data() -> list[dict[str, str]]:
    scenarios = json.loads(TILE_DATA.read_text())["scenarios"]
    assert isinstance(scenarios, list)
    return scenarios


def test_the_tiles_and_the_scenarios_are_the_same_set() -> None:
    assert [tile["id"] for tile in tile_data()] == list(faults.SCENARIOS)


@pytest.mark.parametrize("tile", tile_data(), ids=lambda tile: str(tile["id"]))
def test_every_tile_names_a_test_that_exists(tile: dict[str, str]) -> None:
    path, _, name = tile["test"].partition("::")
    source = REPO_ROOT / path

    assert source.is_file(), f"{tile['id']} names {path}, which is not a file"
    assert re.search(rf"^def {re.escape(name)}\(", source.read_text(), re.MULTILINE), (
        f"{tile['id']} names {name}, which is not a test in {path}"
    )


@pytest.mark.parametrize("tile", tile_data(), ids=lambda tile: str(tile["id"]))
def test_every_tile_says_what_it_does(tile: dict[str, str]) -> None:
    assert tile["title"]
    assert len(tile["detail"]) > 40, "a tile's detail has to say what the damage is"

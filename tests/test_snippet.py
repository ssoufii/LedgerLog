"""The snippet the demo's start dialog hands out, story M11.8.

The page ends with code a visitor copies and runs, so the only test worth
writing is one that runs it. These execute the snippet the page would generate,
for every fsync choice the dialog offers, against a real directory, and then
check the database it claims to have written really holds the record.

The template is read from the same file the page renders, so there is one
snippet and not two: a copy of it in this test would pass while the page handed
out something else.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SNIPPET_DATA = REPO_ROOT / "web" / "src" / "data" / "snippet.json"

DATA = json.loads(SNIPPET_DATA.read_text())
POLICIES: list[str] = DATA["policies"]


def render(directory: str, policy: str) -> str:
    """Render the template the way web/src/data/snippet.ts renders it."""
    escaped = directory.replace("\\", "\\\\").replace('"', '\\"')
    return str(DATA["template"]).replace("__DIRECTORY__", escaped).replace("__POLICY__", policy)


def test_the_template_names_every_policy_the_engine_accepts() -> None:
    from ledgerlog.wal import FsyncPolicy

    assert sorted(POLICIES) == sorted(policy.value for policy in FsyncPolicy)


def test_the_template_has_both_placeholders() -> None:
    assert "__DIRECTORY__" in DATA["template"]
    assert "__POLICY__" in DATA["template"]


@pytest.mark.parametrize("policy", POLICIES)
def test_the_snippet_runs_and_writes_the_record_it_claims(tmp_path: Path, policy: str) -> None:
    directory = tmp_path / "data"
    source = render(str(directory), policy)

    # A subprocess, not exec, so the snippet is run the way a visitor runs it:
    # as a file, from the top, with nothing from this test in its namespace.
    script = tmp_path / "snippet.py"
    script.write_text(source)
    finished = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
    )

    assert finished.returncode == 0, finished.stderr
    assert finished.stdout.strip() == "b'2026-11-16T05:00:00Z'"

    # And it left a real database behind, not just a directory.
    from ledgerlog import LedgerLog

    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as reopened:
        assert reopened.get(b"user:1042:login") == b"2026-11-16T05:00:00Z"


def test_the_snippet_uses_the_constructor_this_engine_actually_has() -> None:
    import inspect

    from ledgerlog import LedgerLog

    source = render("./data", "always")
    parameters = inspect.signature(LedgerLog.__init__).parameters

    assert "fsync_policy" in parameters, "the snippet passes a parameter the engine dropped"
    assert "LedgerLog(" in source
    assert "fsync_policy=" in source
    # put and get take and return bytes, which is what the snippet shows.
    assert 'db.put(b"' in source
    assert 'db.get(b"' in source


def test_a_directory_with_a_quote_in_it_still_parses(tmp_path: Path) -> None:
    import ast

    awkward = str(tmp_path / 'a "quoted" \\ path')

    ast.parse(render(awkward, "never"))

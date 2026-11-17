"""The start dialog's snippet runs, for every choice the dialog offers. Story M11.8.

The web demo ends with a "new database" dialog that writes the code a visitor
needs and copies it. A snippet that names a parameter the constructor does not
have would be worse than no snippet, so this module fills in the same template
the page fills in (``web/src/data/snippets.json``), the same way, and executes
it against the real engine.

The fill-in is repeated here rather than shared with the TypeScript because it
is one ``str.replace`` per placeholder with a JSON string literal, which both
languages do natively; what has to be shared is the template, and it is.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from typing import Any

import pytest

from ledgerlog.wal import FsyncPolicy

SNIPPETS = Path(__file__).resolve().parents[1] / "web" / "src" / "data" / "snippets.json"


def load() -> dict[str, Any]:
    """The template file the page imports."""
    return json.loads(SNIPPETS.read_text(encoding="utf-8"))


def render(directory: str, policy: str) -> str:
    """Fill the template exactly as ``web/src/data/snippet.ts`` does."""
    template: str = load()["template"]
    return template.replace("{{directory}}", json.dumps(directory)).replace(
        "{{fsync_policy}}", json.dumps(policy)
    )


def run(source: str) -> str:
    """Execute a snippet as a fresh module would, and return what it printed."""
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        exec(compile(source, "<snippet>", "exec"), {"__name__": "__snippet__"})
    return printed.getvalue()


def test_the_dialog_offers_exactly_the_engines_fsync_policies() -> None:
    """A choice the engine does not accept would be a snippet that raises."""
    data = load()
    assert sorted(data["policies"]) == sorted(policy.value for policy in FsyncPolicy)
    assert data["default_policy"] in data["policies"]


@pytest.mark.parametrize("policy", [policy.value for policy in FsyncPolicy])
def test_the_snippet_runs_for_every_fsync_choice(tmp_path: Path, policy: str) -> None:
    """Each generated snippet opens a database, writes, reads back, and closes."""
    directory = tmp_path / "data"

    printed = run(render(str(directory), policy))

    assert printed.strip() == repr(b"hello")
    # The database it created is a real one: a second engine opened on the same
    # directory replays the write the snippet made.
    from ledgerlog import LedgerLog

    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as reopened:
        assert reopened.get(b"user:1042") == b"hello"


def test_the_default_directory_is_the_relative_one_the_page_shows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The snippet as first shown, unedited, runs from wherever it is pasted."""
    data = load()
    monkeypatch.chdir(tmp_path)

    printed = run(render(data["default_directory"], data["default_policy"]))

    assert printed.strip() == repr(b"hello")
    assert (tmp_path / data["default_directory"]).is_dir()


def test_a_directory_with_quotes_and_backslashes_is_quoted_not_broken(tmp_path: Path) -> None:
    """The directory field is free text, so the literal must survive anything typed."""
    awkward = tmp_path / 'it\'s a "db" \\ here'

    printed = run(render(str(awkward), "never"))

    assert printed.strip() == repr(b"hello")
    assert awkward.is_dir()

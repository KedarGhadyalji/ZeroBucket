"""Tests for the tests' own `.env` loader (tests/_dotenv.py)."""

from __future__ import annotations

from ._dotenv import PREFIX, load_dotenv, parse_dotenv


def test_parse_handles_comments_blank_lines_export_and_quotes():
    text = (
        "# a comment\n"
        "\n"
        "ZEROBUCKET_A=plain\n"
        "export ZEROBUCKET_B=exported\n"
        'ZEROBUCKET_C="double quoted # not a comment"\n'
        "ZEROBUCKET_D='single quoted'\n"
        "ZEROBUCKET_E=value # trailing comment\n"
        "ZEROBUCKET_F=postgresql://u:p%23w@h:5432/db\n"
        "not a setting\n"
        "=novalue\n"
    )
    assert parse_dotenv(text) == {
        "ZEROBUCKET_A": "plain",
        "ZEROBUCKET_B": "exported",
        "ZEROBUCKET_C": "double quoted # not a comment",
        "ZEROBUCKET_D": "single quoted",
        "ZEROBUCKET_E": "value",
        "ZEROBUCKET_F": "postgresql://u:p%23w@h:5432/db",  # %23 and a bare # survive
    }


def test_load_reads_a_utf8_bom_file_written_by_windows_notepad(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_bytes(b"\xef\xbb\xbfZEROBUCKET_TEST_DATABASE_URL=postgresql://x\n")
    environ: dict[str, str] = {}
    assert load_dotenv(env_file, environ) == ["ZEROBUCKET_TEST_DATABASE_URL"]
    assert environ["ZEROBUCKET_TEST_DATABASE_URL"] == "postgresql://x"


def test_value_in_dotenv_wins_over_the_shell(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("ZEROBUCKET_TEST_DATABASE_URL=from-dotenv\n", encoding="utf-8")
    environ = {"ZEROBUCKET_TEST_DATABASE_URL": "from-shell"}
    load_dotenv(env_file, environ)
    assert environ["ZEROBUCKET_TEST_DATABASE_URL"] == "from-dotenv"


def test_only_zerobucket_keys_are_read_and_empty_values_are_ignored(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "PATH=/should/not/be/touched\nZEROBUCKET_BLANK=\nZEROBUCKET_REAL=1\n",
        encoding="utf-8",
    )
    environ = {"ZEROBUCKET_BLANK": "keep-me"}
    assert load_dotenv(env_file, environ) == ["ZEROBUCKET_REAL"]
    assert environ == {"ZEROBUCKET_BLANK": "keep-me", "ZEROBUCKET_REAL": "1"}
    assert PREFIX == "ZEROBUCKET_"


def test_missing_file_is_not_an_error(tmp_path):
    environ: dict[str, str] = {}
    assert load_dotenv(tmp_path / "nope.env", environ) == []
    assert environ == {}

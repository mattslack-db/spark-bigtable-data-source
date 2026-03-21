"""Tests for bigtable_integration_utils path safety."""

from pathlib import Path


def test_safe_credentials_path_accepts_file_inside_project(tmp_path: Path):
    from tests.bigtable_integration_utils import _safe_credentials_path

    creds = tmp_path / "keys" / "sa.json"
    creds.parent.mkdir(parents=True)
    creds.write_text("{}")

    assert _safe_credentials_path(tmp_path, "keys/sa.json") == creds.resolve()


def test_safe_credentials_path_rejects_traversal_via_dotdot(tmp_path: Path):
    from tests.bigtable_integration_utils import _safe_credentials_path

    outside = tmp_path.parent / "outside_credentials.json"
    outside.write_text("{}")
    # Resolved path leaves project_root (tmp_path)
    assert _safe_credentials_path(tmp_path, "../outside_credentials.json") is None


def test_safe_credentials_path_rejects_absolute_outside_project(tmp_path: Path):
    from tests.bigtable_integration_utils import _safe_credentials_path

    elsewhere = tmp_path.parent / "abs_cred.json"
    elsewhere.write_text("{}")
    assert _safe_credentials_path(tmp_path, str(elsewhere.resolve())) is None


def test_safe_credentials_path_accepts_absolute_inside_project(tmp_path: Path):
    from tests.bigtable_integration_utils import _safe_credentials_path

    f = tmp_path / "c.json"
    f.write_text("{}")
    assert _safe_credentials_path(tmp_path, str(f.resolve())) == f.resolve()


def test_safe_credentials_path_missing_file_returns_none(tmp_path: Path):
    from tests.bigtable_integration_utils import _safe_credentials_path

    assert _safe_credentials_path(tmp_path, "nope.json") is None

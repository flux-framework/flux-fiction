from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from flux_fiction import _paths
from flux_fiction.cli import jobtap_path, main


def test_jobtap_plugin_path_prefers_env_override(monkeypatch, tmp_path: Path):
    plugin = tmp_path / "override-jobtap.so"
    plugin.write_text("fake plugin\n", encoding="utf-8")

    monkeypatch.setenv("FLUX_FICTION_JOBTAP_SO", str(plugin))

    assert _paths.jobtap_plugin_path() == plugin.resolve()


def test_jobtap_plugin_path_scans_repo_build_children(monkeypatch, tmp_path: Path):
    fake_repo = tmp_path / "fake-repo"
    fake_module_path = fake_repo / "src" / "flux_fiction" / "_paths.py"
    fake_module_path.parent.mkdir(parents=True)
    fake_module_path.write_text("# fake\n", encoding="utf-8")

    plugin = fake_repo / "build" / "cp312" / "src" / "emu-jobtap.so"
    plugin.parent.mkdir(parents=True)
    plugin.write_text("fake plugin\n", encoding="utf-8")

    monkeypatch.delenv("FLUX_FICTION_JOBTAP_SO", raising=False)
    monkeypatch.setattr(_paths, "__file__", str(fake_module_path))
    monkeypatch.setattr(_paths.resources, "files", lambda _pkg: (_ for _ in ()).throw(RuntimeError("no bundle")))

    assert _paths.jobtap_plugin_path() == plugin.resolve()


def test_jobtap_plugin_path_raises_when_nothing_found(monkeypatch, tmp_path: Path):
    fake_repo = tmp_path / "empty-repo"
    fake_module_path = fake_repo / "src" / "flux_fiction" / "_paths.py"
    fake_module_path.parent.mkdir(parents=True)
    fake_module_path.write_text("# fake\n", encoding="utf-8")

    monkeypatch.delenv("FLUX_FICTION_JOBTAP_SO", raising=False)
    monkeypatch.setattr(_paths, "__file__", str(fake_module_path))
    monkeypatch.setattr(_paths.resources, "files", lambda _pkg: (_ for _ in ()).throw(RuntimeError("no bundle")))

    with pytest.raises(FileNotFoundError):
        _paths.jobtap_plugin_path()


def test_jobtap_cli_main_prints_resolved_path(monkeypatch, capsys):
    plugin = Path("/tmp/fake-jobtap.so")
    monkeypatch.setattr(jobtap_path, "jobtap_plugin_path", lambda: plugin)

    rc = jobtap_path.main()

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() == str(plugin)


def test_cli_build_parser_batch_job_starts_flag_disables_batching():
    parser = main.build_parser()
    args = parser.parse_args(["--batch_job_starts"])
    assert args.batch_job_starts is False


def test_cli_main_returns_zero_on_success(monkeypatch):
    sentinel_args = SimpleNamespace()

    class _Parser:
        def parse_args(self):
            return sentinel_args

    monkeypatch.setattr(main, "build_parser", lambda: _Parser())
    monkeypatch.setattr(
        main.client,
        "run_experiment_from_args",
        lambda args: SimpleNamespace(ok=True, message="done", args=args),
    )

    assert main.main() == 0


def test_cli_main_returns_one_on_failure(monkeypatch):
    sentinel_args = SimpleNamespace()

    class _Parser:
        def parse_args(self):
            return sentinel_args

    monkeypatch.setattr(main, "build_parser", lambda: _Parser())
    monkeypatch.setattr(
        main.client,
        "run_experiment_from_args",
        lambda args: SimpleNamespace(ok=False, message="boom", args=args),
    )

    assert main.main() == 1

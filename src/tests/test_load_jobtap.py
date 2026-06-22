from __future__ import annotations

from pathlib import Path
import stat
import subprocess


def test_load_jobtap_prefers_existing_flux_prefix_environment(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    fake_prefix = tmp_path / "flux-prefix"
    fake_bin = fake_prefix / "bin"
    fake_bin.mkdir(parents=True)
    fake_flux = fake_bin / "flux"
    fake_flux.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'fake flux invoked: %s\\n' \"$*\"\n",
        encoding="ascii",
    )
    fake_flux.chmod(fake_flux.stat().st_mode | stat.S_IXUSR)

    fake_plugin = tmp_path / "emu-jobtap.so"
    fake_plugin.write_text("not-a-real-plugin\n", encoding="ascii")

    command = (
        "set -euo pipefail\n"
        f"export FLUX_PREFIX={fake_prefix}\n"
        f"export FLUX_FICTION_JOBTAP_SO={fake_plugin}\n"
        f"source {repo_root / 'load_jobtap.sh'}\n"
    )
    proc = subprocess.run(
        ["bash", "-lc", command],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert "fake flux invoked: jobtap load" in proc.stdout
    assert str(fake_plugin) in proc.stdout

from __future__ import annotations

from flux_fiction._paths import jobtap_plugin_path


def main() -> int:
    print(jobtap_plugin_path())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

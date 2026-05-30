<p align="center">
  <img src="assets/ff_logo.png" alt="Flux Fiction Logo" width="220">
</p>

# Flux Fiction

Flux Fiction is a trace-driven emulator for experimenting with Flux scheduling
policies without touching a production system. It plugs into real Flux and
Fluxion components, submits emulated jobs, tracks their lifecycle, and writes
artifacts that help you study scheduling behavior, resource use, and timing.

## What It Does

- Replays historical or synthetic workloads against a real Flux instance.
- Uses a native jobtap plugin to emulate job execution and timing.
- Produces run directories with logs, transition tables, resource timelines,
  utilization plots, and trace-style outputs.
- Supports optional faketime-driven simulation control and OpenTelemetry
  profiling.

## Quickstart

The recommended setup is a shared workspace with sibling checkouts:

```text
<workspace>/
  flux-core/
  flux-sched/
  flux-fiction/
```

### 1. Build The Dev Image

```bash
cd flux-fiction/podman_containers
./build_container.sh flux-fiction-dev
```

### 2. Start The Container

From the workspace parent directory:

```bash
podman run --rm -it -v "$(pwd)":/workspace flux-fiction-dev
```

### 3. Build And Install Inside The Container

```bash
source /usr/local/bin/flux-dev-env.sh
/usr/local/bin/build-flux-core.sh
/usr/local/bin/build-flux-sched.sh
/usr/local/bin/build-flux-fiction.sh
```

This gives you an editable install of `flux-fiction` and the main CLI commands:

- `flux-fiction`
- `flux-fiction-run`
- `flux-fiction-jobtap-path`

### 4. Run A Smoke Test

```bash
cd /workspace/flux-fiction
flux-fiction-run test/simple_test/config.toml --tag smoke --no-faketime
```

### 5. Run Tests

```bash
cd /workspace/flux-fiction
pytest -q
```

## Main Commands

Use the base CLI when you already have a config and environment prepared:

```bash
flux-fiction --config_file /path/to/config.toml
```

Use the harness when you want Flux Fiction to prepare a run directory, copy
config inputs, wire faketime, and launch a fresh Flux instance:

```bash
flux-fiction-run /path/to/config.toml
```

Locate the built or installed jobtap plugin:

```bash
flux-fiction-jobtap-path
```

## Dependencies

### Python Package Dependencies

Declared in `pyproject.toml`:

- `pydantic`
- `tqdm`

Optional extras:

- `dev`: `build`, `meson`, `meson-python`, `pytest`, `pytest-cov`
- `plot`: `matplotlib`
- `otel`: `opentelemetry-api`, `opentelemetry-sdk`,
  `opentelemetry-exporter-otlp-proto-http`

### Native And System Dependencies

Required to build and run the jobtap plugin and the full simulator stack:

- `flux-core` headers and libraries
- `flux-sched`
- `jansson`
- `meson`
- `ninja`
- `pkg-config`
- `gcc` / `g++`
- `cmake`
- `python3`, `pip`, and Python development headers

### Dev Container Dependencies

The Podman dev image additionally includes tooling commonly needed for local
development and CI setup:

- `pytest`
- `pytest-cov`
- `matplotlib`
- `meson-python`
- `build`
- OpenTelemetry Python packages
- debugging and build tools such as `gdb`, `valgrind`, `strace`, `tmux`

## Local Python Install

If you already have Flux and the native build dependencies available on your
system, you can install directly from the repo.

### Development Install

```bash
python3 -m pip install -e '.[dev]'
```

### Install With Plotting And OTel Extras

```bash
python3 -m pip install -e '.[dev,plot,otel]'
```

## Native Build

Flux Fiction uses Meson for the native `emu-jobtap.so` build.

### Direct Meson Build

```bash
meson setup builddir
meson compile -C builddir
meson install -C builddir
```

### Point Meson At A Flux Install

```bash
meson setup builddir -Dflux_prefix=/path/to/flux-core
```

In the shared Podman workspace used by this project, Meson will also
automatically try:

- `../container-installs/flux-core`
- `../flux-core`

## Outputs

A typical run directory contains artifacts like:

- `run.log`
- `broker.log`
- `emu.log`
- `job_transitions.csv`
- `eventlog.csv`
- `resource_usage_timeseries.csv`
- `resource_allocations.csv`
- `resource_utilization.png` or `resource_utilization.svg`
- `pernode.json`

## Profiling

OpenTelemetry profiling is supported through `flux-fiction-run --otel`.

Example:

```bash
cd /workspace/flux-fiction
flux-fiction-run \
  test/rabbit_storage_test/config_queue20_otel_debug.toml \
  --otel \
  --otel-service-name rabbit-tuolumne-profile \
  --tag queue20-otel
```

Important notes:

- A live OTLP collector is optional for local profiling.
- Local profiling artifacts are still written even if `127.0.0.1:4318` is not
  available.
- Common local outputs include `otel_spans.jsonl`, `otel_bridge.log`, and
  sometimes `otel_summary.csv`.

## Repository Notes

- `util/run_ff.py` remains as a source-tree compatibility wrapper around the
  installed `flux-fiction-run` harness.
- `load_jobtap.sh` will load the local build artifact or the installed plugin
  path helper.
- The preferred development path is the Podman container because it keeps Flux,
  scheduler, Python, and native dependencies aligned.

## Auspices

This work was supported by the LLNL-LDRD Program under Project No. 24-SI-005.

## Reference

Please use this bibtex to reference the project:

```bibtex
@inproceedings{HPDCFluxEmulatorPoster,
  author    = {W. Jay Ashworth and Ian Lumsden and Jim Garlick and Mark Grondona and Olga Pearce and Stephanie Brink and Dewi Yokelson and Daniel Milroy and Tapasya Patki and Tom Scogland and Michela Taufer},
  title     = {{Poster: Flux Emulator: First Insights into Optimizing Scheduling for Exascale HPC}},
  booktitle = {Proceedings of the International ACM Symposium on High-Performance Parallel and Distributed Computing (HPDC)},
  year      = {2025},
  month     = {July},
  address   = {Notre Dame, IN, USA},
  note      = {Poster Presentation}
}
```

## License

SPDX-License-Identifier: LGPL-3.0

LLNL-CODE-764420

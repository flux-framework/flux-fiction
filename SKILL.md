# Flux Fiction Podman Skill

Use this guide when working on `flux-fiction` through the Podman-based development container.

## Scope

This repository is developed together with sibling checkouts of:

- `flux-core`
- `flux-sched`
- `flux-fiction`
- `podman_containers`

The container expects those repos to be mounted together under `/workspace`.

## Workspace Layout

On the host, the parent directory should contain:

```text
<workspace>/
  flux-core/
  flux-sched/
  flux-fiction/
  podman_containers/
```

Inside the container, that same parent directory must be mounted at:

```text
/workspace
```

The installed Flux prefix inside the container is:

```text
/workspace/container-installs/flux-core
```

## Build The Image

The OCI image is defined in:

- `podman_containers/Containerfile`

Build it with:

```bash
cd podman_containers
./build_container.sh flux-fiction-dev
```

Important:

- `build_container.sh` currently uses an absolute host path to `podman_containers`.
- If the workspace moves to a different host path, update that script or run `podman build` manually.

Equivalent manual build:

```bash
podman build -t flux-fiction-dev -f podman_containers/Containerfile podman_containers
```

## Start The Container

From the host workspace parent directory:

```bash
podman run --rm -it -v "$(pwd)":/workspace flux-fiction-dev
```

If not already in the workspace parent directory, mount it explicitly:

```bash
podman run --rm -it -v /path/to/workspace:/workspace flux-fiction-dev
```

## In-Container Environment

The image provides:

- `/usr/local/bin/flux-dev-env.sh`
- `/usr/local/bin/build-flux-core.sh`
- `/usr/local/bin/build-flux-sched.sh`
- `/usr/local/bin/build-flux-fiction.sh`
- `/usr/local/bin/build-flux-stack.sh`

The shell profile also defines aliases:

- `rebuild-flux-core`
- `rebuild-flux-sched`
- `rebuild-flux-fiction`
- `rebuild-flux-stack`

In non-interactive shells, prefer the script paths directly because aliases may not be expanded.

Before manual work, it is safe to source:

```bash
source /usr/local/bin/flux-dev-env.sh
```

That script also configures Git safe directories for the mounted repos.

## Rebuild Workflow

Use these commands inside the container:

```bash
/usr/local/bin/build-flux-core.sh
/usr/local/bin/build-flux-sched.sh
/usr/local/bin/build-flux-fiction.sh
```

Or the whole stack:

```bash
/usr/local/bin/build-flux-stack.sh
```

Notes:

- `flux-core` installs into `/workspace/container-installs/flux-core`
- `flux-sched` installs into the same prefix
- `flux-fiction` builds `build/emu-jobtap.so`

## Load The Jobtap Plugin

From the `flux-fiction` repo inside the container:

```bash
cd /workspace/flux-fiction
source ./load_jobtap.sh
```

That script expects the plugin at:

```text
/workspace/flux-fiction/build/emu-jobtap.so
```

## Run Flux Fiction

The standard harness is:

- `flux-fiction/util/run_ff.py`

Simple smoke run:

```bash
cd /workspace/flux-fiction
python3 util/run_ff.py /workspace/flux-fiction/test/simple_test/config.toml --tag smoke
```

If faketime is causing trouble during debugging:

```bash
python3 util/run_ff.py /workspace/flux-fiction/test/simple_test/config.toml --tag smoke --no-faketime
```

## OpenTelemetry Profiling

The image already includes the needed Python OTel packages.

Use `run_ff.py --otel`. It starts the bridge process outside the faketime-preloaded runtime and writes profiling artifacts into the run directory.

Typical rabbit/Tuolumne profiling run:

```bash
cd /workspace/flux-fiction
python3 util/run_ff.py \
  /workspace/flux-fiction/test/rabbit_storage_test/config_queue20_otel_debug.toml \
  --otel \
  --otel-service-name rabbit-tuolumne-profile \
  --tag queue20-otel
```

Important facts:

- A live OTLP collector is optional for local profiling.
- The bridge still writes local files even if `127.0.0.1:4318` is unavailable.
- The main local outputs are:
  - `otel_spans.jsonl`
  - `otel_bridge.log`
  - sometimes `otel_summary.csv`

For jobtap-side `sched.quiescent` timing, inspect spans named:

- `jobtap.sched_quiescent_wait`
- `jobtap.sched_quiescent_continuation`
- `simulation.query_quiescent`

## Known Caveats

- `util/run_ff.py` is the preferred entry point for runs; it prepares config copies, faketime, reproducer scripts, and OTel wiring.
- `build_container.sh` is not yet portable because it hardcodes a host path.
- In non-interactive container commands, aliases may not exist; use `/usr/local/bin/build-flux-*.sh`.
- `flux-sched` versioning may fall back to `0.0.0` in the container if Git tags are unavailable.
- `load_jobtap.sh` is a real loader now; do not replace it with a no-op stub.

## Recommended Agent Workflow

1. Confirm the host workspace contains all four sibling repos.
2. Rebuild the image only if `Containerfile` or OS-level dependencies changed.
3. Start the container with the workspace mounted to `/workspace`.
4. Source `/usr/local/bin/flux-dev-env.sh`.
5. Rebuild only the component you changed.
6. Use `python3 util/run_ff.py ...` for testing, not ad hoc broker commands.
7. For scheduler timing questions, prefer the rabbit OTel configs and inspect local JSONL spans first.

# flux-fiction

This repo contains Flux Fiction, a tool designed to test scheduling policies in Flux without impacting production systems. The emulator plugs into the real components of Flux and Fluxion to mimic job execution, emulate resource usage, and collect information on how the jobs are treated by Flux. 

## Build And Install

Flux Fiction now uses a standard `pyproject.toml` and Meson-based native build.

### Fresh Clone

The recommended first-time setup is the shared Podman workspace used by this
project. Arrange sibling checkouts like this:

```text
<workspace>/
  flux-core/
  flux-sched/
  flux-fiction/
```

Build the dev image from `flux-fiction/podman_containers/`:

```bash
cd flux-fiction/podman_containers
./build_container.sh flux-fiction-dev
```

Start the container with the whole workspace mounted at `/workspace`:

```bash
cd ..
podman run --rm -it -v "$(pwd)":/workspace flux-fiction-dev
```

Inside the container:

```bash
source /usr/local/bin/flux-dev-env.sh
/usr/local/bin/build-flux-core.sh
/usr/local/bin/build-flux-sched.sh
/usr/local/bin/build-flux-fiction.sh
```

That last step performs an editable install of `flux-fiction`, builds the
native jobtap plugin through Meson, and makes these commands available:

- `flux-fiction`
- `flux-fiction-run`
- `flux-fiction-jobtap-path`

Run a smoke test:

```bash
cd /workspace/flux-fiction
flux-fiction-run test/simple_test/config.toml --tag smoke --no-faketime
```

Run Python tests:

```bash
cd /workspace/flux-fiction
pytest -q
```

### Python package

Install for development:

```bash
python3 -m pip install -e '.[dev]'
```

Install optional extras when needed:

```bash
python3 -m pip install -e '.[dev,plot,otel]'
```

This installs the `flux-fiction` console entry point and the
`flux-fiction-jobtap-path` helper used to locate the bundled `emu-jobtap.so`
plugin.

### Native plugin build

The `emu-jobtap.so` plugin is built by Meson and installed into the Python
package under `flux_fiction/_native/`.

Direct Meson build:

```bash
meson setup builddir
meson compile -C builddir
meson install -C builddir
```

If `flux-core` is not in a standard system prefix, point Meson at the install:

```bash
meson setup builddir -Dflux_prefix=/path/to/flux-core
```

In the shared Podman workspace used by this project, Meson will also
automatically try:

- `../container-installs/flux-core`
- `../flux-core`

### Runtime dependencies

The Python package declares pure-Python dependencies in `pyproject.toml`.
System-level dependencies for the native jobtap plugin still need to be present,
including:

- `flux-core` headers and libraries
- `jansson`

After installation, the main CLI can be invoked without `python -m`:

```bash
flux-fiction --config_file /path/to/config.toml
```

The harness that prepares run directories and launches a fresh Flux instance is:

```bash
flux-fiction-run /path/to/config.toml
```

#### Auspices

This work was supported by the LLNL-LDRD Program under Project No. 24-SI-005.

#### References

Please use this bibtex to reference our work:

```
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

#### License

SPDX-License-Identifier: LGPL-3.0

LLNL-CODE-764420

#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# run_all_experiments.sh
#
# Launches 36 Flux Fiction experiments (9 workloads × 4 scheduling policies).
# Each workload gets its own tmux window running a dedicated Flux instance.
# Within each instance, the 4 policies run sequentially.
#
# Usage:
#   chmod +x run_all_experiments.sh
#   ./run_all_experiments.sh
#
# Prerequisites:
#   - tmux installed
#   - flux available on PATH (or via the start_flux.sh environment)
#   - Workload CSVs already generated in WORKLOAD_DIR
# ============================================================================

# ── Paths ────────────────────────────────────────────────────────────────────
FF_ROOT="/home/j/Desktop/flux/sc25_poster/flux-fiction"
WORKLOAD_DIR="${FF_ROOT}/vis/workloads"
ORIGINAL_WORKLOAD="${FF_ROOT}/experiment_data/ff_traces/experiment_tuo-default_20260330_130030/processed/results_submit_order.csv"
EXPERIMENT_BASE="${FF_ROOT}/experiment_data/sweep_$(date +%Y%m%d_%H%M%S)"
START_FLUX="${FF_ROOT}/util/start_flux.sh"
SRC_DIR="${FF_ROOT}/src"

TMUX_SESSION="ff_sweep"

# ── System parameters (must match your Flux Fiction cluster config) ──────────
NNODES=128
NSOCKETS=1
NCPUS=96
NGPUS=0

# ── Scheduling policies ─────────────────────────────────────────────────────
# Format: "policy_name:queue_policy:reservation_depth"
POLICIES=(
    "easy:easy:32"
    "conservative:conservative:32"
    "hybrid:hybrid:4"
    "fcfs:fcfs:32"
)

# ── Build the list of workloads ──────────────────────────────────────────────
# Each entry: "short_name:path_to_csv"
WORKLOADS=()
WORKLOADS+=("original:${ORIGINAL_WORKLOAD}")

for csv_file in "${WORKLOAD_DIR}"/flux_fiction_*.csv; do
    [ -f "$csv_file" ] || continue
    base=$(basename "$csv_file" .csv)
    # Strip the "flux_fiction_" prefix to get a clean tag
    tag="${base#flux_fiction_}"
    WORKLOADS+=("${tag}:${csv_file}")
done

echo "============================================================"
echo "Flux Fiction Experiment Sweep"
echo "============================================================"
echo "Experiment root : ${EXPERIMENT_BASE}"
echo "Workloads found : ${#WORKLOADS[@]}"
echo "Policies        : ${#POLICIES[@]}"
echo "Total experiments: $(( ${#WORKLOADS[@]} * ${#POLICIES[@]} ))"
echo "------------------------------------------------------------"

if [ "${#WORKLOADS[@]}" -eq 0 ]; then
    echo "ERROR: No workload CSVs found in ${WORKLOAD_DIR}"
    exit 1
fi

# ── Generate config files for every experiment ───────────────────────────────
echo ""
echo "Generating experiment directories and configs..."

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name wl_path <<< "$wl_entry"

    for pol_entry in "${POLICIES[@]}"; do
        IFS=':' read -r pol_name queue_policy res_depth <<< "$pol_entry"

        exp_dir="${EXPERIMENT_BASE}/${wl_name}/${pol_name}"
        mkdir -p "$exp_dir"

        # ── config.json (scheduling policy) ──────────────────────────
        cat > "${exp_dir}/config.json" <<JSONEOF
{
  "sched-fluxion-qmanager": {
    "queue-policy": "${queue_policy}",
    "queue-params": {
      "queue-depth": 1024
    },
    "policy-params": {
      "reservation-depth": ${res_depth}
    }
  },
  "sched-fluxion-resource": {
    "match-policy": "lonodex",
    "prune-filters": "ALL:core",
    "match-format": "rv1_nosched"
  }
}
JSONEOF

        # ── config.toml (Flux Fiction input) ─────────────────────────
        cat > "${exp_dir}/config.toml" <<TOMLEOF
[flux_fiction]
job_traces = "${wl_path}"
nnodes = ${NNODES}
nsockets = ${NSOCKETS}
ncpus = ${NCPUS}
ngpus = ${NGPUS}
log_level = 10
log_file = "emu.log"
backend = "flux"
quiet = true
batch_job_starts = false
output_dir = "${exp_dir}/output"
config_json = "${exp_dir}/config.json"
TOMLEOF

        mkdir -p "${exp_dir}/output"
        echo "  ✓ ${wl_name} / ${pol_name}"
    done
done

# ── Generate the per-workload runner script ──────────────────────────────────
# Each workload gets a small script that runs its 4 policies sequentially
# inside a single Flux instance.

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name wl_path <<< "$wl_entry"

    runner="${EXPERIMENT_BASE}/${wl_name}/run_policies.sh"
    cat > "$runner" <<'RUNEOF'
#!/usr/bin/env bash
set -euo pipefail

WL_NAME="$1"
EXPERIMENT_BASE="$2"
POLICIES_STR="$3"

WL_DIR="${EXPERIMENT_BASE}/${WL_NAME}"

echo "[$(date '+%H:%M:%S')] Starting policies for workload: ${WL_NAME}"

IFS='|' read -ra POLS <<< "$POLICIES_STR"
for pol_name in "${POLS[@]}"; do
    exp_dir="${WL_DIR}/${pol_name}"
    config_toml="${exp_dir}/config.toml"

    echo "[$(date '+%H:%M:%S')]   Running policy: ${pol_name}"
    echo "[$(date '+%H:%M:%S')]   Config: ${config_toml}"

    python -m flux_fiction.cli.main --config_file "${config_toml}" 2>&1 | \
        tee "${exp_dir}/run.log"

    echo "[$(date '+%H:%M:%S')]   ✓ ${pol_name} complete"
    echo ""
done

echo "[$(date '+%H:%M:%S')] All policies complete for ${WL_NAME}"
RUNEOF
    chmod +x "$runner"
done

# ── Launch tmux sessions ─────────────────────────────────────────────────────
echo ""
echo "Launching ${#WORKLOADS[@]} Flux instances via tmux..."
echo "Session name: ${TMUX_SESSION}"
echo ""

# Kill any existing session with this name
tmux kill-session -t "${TMUX_SESSION}" 2>/dev/null || true

# Build the policies list as a pipe-delimited string for passing to runners
POL_NAMES=""
for pol_entry in "${POLICIES[@]}"; do
    IFS=':' read -r pol_name _ _ <<< "$pol_entry"
    if [ -z "$POL_NAMES" ]; then
        POL_NAMES="$pol_name"
    else
        POL_NAMES="${POL_NAMES}|${pol_name}"
    fi
done

FIRST=true
WINDOW_IDX=0

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name wl_path <<< "$wl_entry"

    runner="${EXPERIMENT_BASE}/${wl_name}/run_policies.sh"
    logfile="${EXPERIMENT_BASE}/${wl_name}/broker-logs.txt"

    # The command we run inside each tmux window:
    # 1. cd to the util directory
    # 2. Start a Flux instance with a unique broker log
    # 3. Inside that instance, cd to src and run the policies sequentially
    #
    # We inline the start_flux logic here so we can customize the broker log
    # path and inject the sequential policy runner.
    flux_cmd="cd ${FF_ROOT}/util && \
LOGFILE='${logfile}' && \
rm -f \"\$LOGFILE\" && \
flux start --broker-opts=\"--setattr=log-filename=\$LOGFILE\" -- bash -lc ' \
  cd ${FF_ROOT} && source ./util/../load_jobtap.sh 2>/dev/null; \
  cd ${SRC_DIR} && \
  ${runner} ${wl_name} ${EXPERIMENT_BASE} \"${POL_NAMES}\" && \
  echo \"\" && \
  echo \"=== DONE: ${wl_name} — all policies finished ===\" && \
  echo \"You may close this window.\" && \
  exec bash -i \
'"

    if $FIRST; then
        tmux new-session -d -s "${TMUX_SESSION}" -n "${wl_name}" \
            bash -c "$flux_cmd"
        FIRST=false
    else
        tmux new-window -t "${TMUX_SESSION}" -n "${wl_name}" \
            bash -c "$flux_cmd"
    fi

    echo "  Window ${WINDOW_IDX}: ${wl_name} (${logfile})"
    WINDOW_IDX=$((WINDOW_IDX + 1))
done

echo ""
echo "============================================================"
echo "All ${#WORKLOADS[@]} Flux instances launched!"
echo ""
echo "  tmux attach -t ${TMUX_SESSION}"
echo ""
echo "Each window runs 4 policies sequentially."
echo "Results will be in:"
echo "  ${EXPERIMENT_BASE}/<workload>/<policy>/output/"
echo ""
echo "Monitor progress:"
echo "  watch -n5 'find ${EXPERIMENT_BASE} -name run.log -exec tail -1 {} \\;'"
echo "============================================================"

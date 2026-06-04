#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# run_snb_experiments.sh
#
# Re-runs only the 4 small-node bias workloads × 4 scheduling policies (16 experiments).
# Assumes workload CSVs have already been regenerated with tighter submit gaps.
# ============================================================================

FF_ROOT="/home/j/Desktop/flux/sc25_poster/flux-fiction"
WORKLOAD_DIR="${FF_ROOT}/vis/workloads"
EXPERIMENT_BASE="${FF_ROOT}/experiment_data/sweep_snb_$(date +%Y%m%d_%H%M%S)"
SRC_DIR="${FF_ROOT}/src"

TMUX_SESSION="ff_sweep_snb"

NNODES=128
NSOCKETS=1
NCPUS=96
NGPUS=0

POLICIES=(
    "easy:easy:32"
    "conservative:conservative:32"
    "hybrid:hybrid:4"
    "fcfs:fcfs:32"
)

# Only small-node bias workloads
WORKLOADS=()
for csv_file in "${WORKLOAD_DIR}"/flux_fiction_small-node_bias_*.csv; do
    [ -f "$csv_file" ] || continue
    base=$(basename "$csv_file" .csv)
    tag="${base#flux_fiction_}"
    WORKLOADS+=("${tag}:${csv_file}")
done

echo "============================================================"
echo "Flux Fiction — Small-Node Bias Re-run"
echo "============================================================"
echo "Experiment root : ${EXPERIMENT_BASE}"
echo "Workloads found : ${#WORKLOADS[@]}"
echo "Policies        : ${#POLICIES[@]}"
echo "Total experiments: $(( ${#WORKLOADS[@]} * ${#POLICIES[@]} ))"
echo "------------------------------------------------------------"

if [ "${#WORKLOADS[@]}" -eq 0 ]; then
    echo "ERROR: No small_node_bias CSVs found in ${WORKLOAD_DIR}"
    exit 1
fi

# ── Generate configs ─────────────────────────────────────────────────────────
echo ""
echo "Generating experiment directories and configs..."

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name wl_path <<< "$wl_entry"

    for pol_entry in "${POLICIES[@]}"; do
        IFS=':' read -r pol_name queue_policy res_depth <<< "$pol_entry"

        exp_dir="${EXPERIMENT_BASE}/${wl_name}/${pol_name}"
        mkdir -p "${exp_dir}/output"

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

        echo "  ✓ ${wl_name} / ${pol_name}"
    done
done

# ── Per-workload runner scripts ──────────────────────────────────────────────
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

# ── Build policy name string ─────────────────────────────────────────────────
POL_NAMES=""
for pol_entry in "${POLICIES[@]}"; do
    IFS=':' read -r pol_name _ _ <<< "$pol_entry"
    if [ -z "$POL_NAMES" ]; then
        POL_NAMES="$pol_name"
    else
        POL_NAMES="${POL_NAMES}|${pol_name}"
    fi
done

# ── Launch tmux ──────────────────────────────────────────────────────────────
echo ""
echo "Launching ${#WORKLOADS[@]} Flux instances via tmux..."

tmux kill-session -t "${TMUX_SESSION}" 2>/dev/null || true

FIRST=true
WINDOW_IDX=0

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name wl_path <<< "$wl_entry"

    runner="${EXPERIMENT_BASE}/${wl_name}/run_policies.sh"
    logfile="${EXPERIMENT_BASE}/${wl_name}/broker-logs.txt"

    flux_cmd="cd ${FF_ROOT}/util && \
LOGFILE='${logfile}' && \
rm -f \"\$LOGFILE\" && \
flux start --broker-opts=\"--setattr=log-filename=\$LOGFILE\" -- bash -lc ' \
  cd ${FF_ROOT} && source ./load_jobtap.sh 2>/dev/null; \
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

    echo "  Window ${WINDOW_IDX}: ${wl_name}"
    WINDOW_IDX=$((WINDOW_IDX + 1))
done

echo ""
echo "============================================================"
echo "Launched ${#WORKLOADS[@]} SNB workloads in tmux session: ${TMUX_SESSION}"
echo ""
echo "  tmux attach -t ${TMUX_SESSION}"
echo ""
echo "Results: ${EXPERIMENT_BASE}/<workload>/<policy>/output/"
echo "============================================================"

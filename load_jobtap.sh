#!/usr/bin/env bash

_load_jobtap_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_load_jobtap_nounset=0
case "$-" in
    *u*)
        _load_jobtap_nounset=1
        set +u
        ;;
esac

if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then
    source /usr/local/bin/flux-dev-env.sh
elif [[ -n "${FLUX_PREFIX:-}" ]]; then
    export PATH="${FLUX_PREFIX}/bin:${PATH}"
    export LD_LIBRARY_PATH="${FLUX_PREFIX}/lib:${FLUX_PREFIX}/lib64:${LD_LIBRARY_PATH:-}"
    export PKG_CONFIG_PATH="${FLUX_PREFIX}/lib/pkgconfig:${FLUX_PREFIX}/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

if [[ "${_load_jobtap_nounset}" -eq 1 ]]; then
    set -u
fi

_load_jobtap_so="${FLUX_FICTION_JOBTAP_SO:-${_load_jobtap_dir}/build/emu-jobtap.so}"
if [[ ! -f "${_load_jobtap_so}" ]] && compgen -G "${_load_jobtap_dir}/build/*/src/emu-jobtap.so" >/dev/null; then
    _load_jobtap_so="$(printf '%s\n' "${_load_jobtap_dir}"/build/*/src/emu-jobtap.so | head -n 1)"
fi
if [[ ! -f "${_load_jobtap_so}" ]] && command -v flux-fiction-jobtap-path >/dev/null 2>&1; then
    _load_jobtap_so="$(flux-fiction-jobtap-path)"
fi
if [[ ! -f "${_load_jobtap_so}" ]]; then
    echo "ERROR: jobtap plugin not found: ${_load_jobtap_so}" >&2
    return 1 2>/dev/null || exit 1
fi

flux jobtap load "${_load_jobtap_so}"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   

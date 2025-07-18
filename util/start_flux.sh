LOGFILE="./broker-logs.txt"
CLEAN=true

for arg in "$@"; do
  case "$arg" in
    --no-clean)
      CLEAN=false
      ;;
    *)
      echo "Unknown option: $arg"
      echo "Usage: $0 [--no-clean]"
      exit 1
      ;;
  esac
done

if $CLEAN; then
  if [ -f "$LOGFILE" ]; then
    rm "$LOGFILE" || {
      echo "ERROR: could not remove old log at $LOGFILE"
      exit 1
    }
    echo "Removed old log: $LOGFILE"
  fi
fi

flux start --broker-opts="--setattr=log-filename=$LOGFILE"
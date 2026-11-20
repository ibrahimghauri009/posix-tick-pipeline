#!/usr/bin/env bash
# run.sh — Master orchestration script for the OS Final Project pipeline.
# Usage: ./run.sh -i <input_dir> -o <output_dir> [-n <threads>] [-q <queue>] [-c] [-h]
# Requires: gcc, make
# Passes: shellcheck

set -euo pipefail

#  Defaults
INPUT_DIR=""
OUTPUT_DIR="report"
NUM_THREADS=4
QUEUE_SIZE=16
DO_CLEAN=0
PID_FILE=".dispatcher.pid"
START_TIME=""

# Colour helpers
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# Function: print_help
print_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  -i <dir>    Input directory containing CSV files (required)
  -o <dir>    Output directory for report files    (default: output)
  -n <int>    Number of worker threads             (default: 4)
  -q <int>    Bounded queue size                   (default: 16)
  -c          Clean build artifacts before running
  -h          Show this help message

Example:
  $0 -i ./data -o ./output -n 4 -q 16
EOF
}

# Function: check_dependencies 
check_dependencies() {
    local missing=0
    for tool in gcc make; do
        if ! command -v "$tool" &>/dev/null; then
            error "Required tool '$tool' is not installed."
            missing=1
        fi
    done
    if [[ "$missing" -eq 1 ]]; then
        exit 10
    fi
    info "Dependencies OK (gcc, make found)"
}

# Function: check_input_dir
check_input_dir() {
    if [[ -z "$INPUT_DIR" ]]; then
        error "Input directory not specified. Use -i <dir>"
        print_help
        exit 10
    fi
    if [[ ! -d "$INPUT_DIR" ]]; then
        error "Input directory '$INPUT_DIR' does not exist."
        exit 40
    fi

    local csv_count
    csv_count=$(find "$INPUT_DIR" -maxdepth 1 -name "*.csv" | wc -l)
    if [[ "$csv_count" -lt 1 ]]; then
        error "No .csv files found in '$INPUT_DIR'."
        exit 40
    fi
    info "Found $csv_count CSV file(s) in '$INPUT_DIR'"
}

#  Function: build_project 
build_project() {
    if [[ "$DO_CLEAN" -eq 1 ]]; then
        info "Cleaning previous build..."
        make clean
    fi

    info "Building project..."
    if ! make; then
        error "Build failed. Fix compilation errors and retry."
        exit 10
    fi
    info "Build successful"
}

# Function: print_summary
print_summary() {
    local end_time
    end_time=$(date +%s)
    local runtime=$(( end_time - START_TIME ))

    echo ""
    echo "---------------------------------------------"
    echo "  PIPELINE SUMMARY"
    echo "---------------------------------------------"
    printf "  Total runtime   : %d seconds\n" "$runtime"

    local csv_out="${OUTPUT_DIR}/report.csv"
    if [[ -f "$csv_out" ]]; then
        # count data lines (skip header)
        local records
        records=$(tail -n +2 "$csv_out" | grep -c '.' || true)
        printf "  Symbols reported: %d\n" "$records"
        echo "  Output files:"
        echo "    - ${OUTPUT_DIR}/report.txt"
        echo "    - ${OUTPUT_DIR}/report.csv"
    else
        warn "report.csv not found — pipeline may not have completed."
    fi

    echo "============================================="
}

# Trap: cleanup on exit / interrupt
cleanup() {
    local exit_code=$?

    # kill dispatcher if still running
    if [[ -f "$PID_FILE" ]]; then
        local dpid
        dpid=$(cat "$PID_FILE")
        if kill -0 "$dpid" 2>/dev/null; then
            warn "Sending SIGTERM to dispatcher (PID=$dpid) ..."
            kill "$dpid" 2>/dev/null || true
            sleep 1
        fi
        rm -f "$PID_FILE"
    fi

    # clean up any stale IPC
    rm -f /tmp/os_project_fifo 2>/dev/null || true

    if [[ "$exit_code" -eq 0 ]]; then
        print_summary
    else
        warn "Pipeline exited with code $exit_code"
    fi
}

trap cleanup EXIT INT TERM

# Parse options with getopts
while getopts ":i:o:n:q:ch" opt; do
    case "$opt" in
        i) INPUT_DIR="$OPTARG"          ;;
        o) OUTPUT_DIR="$OPTARG"         ;;
        n) NUM_THREADS="$OPTARG"        ;;
        q) QUEUE_SIZE="$OPTARG"         ;;
        c) DO_CLEAN=1                   ;;
        h) print_help; exit 0           ;;
        :) error "Option -$OPTARG requires an argument."; exit 10 ;;
        \?) error "Unknown option: -$OPTARG"; exit 10             ;;
    esac
done

# Validate numeric inputs
if ! [[ "$NUM_THREADS" =~ ^[0-9]+$ ]] || [[ "$NUM_THREADS" -lt 1 ]]; then
    error "Number of threads must be a positive integer."
    exit 10
fi
if ! [[ "$QUEUE_SIZE" =~ ^[0-9]+$ ]] || [[ "$QUEUE_SIZE" -lt 1 ]]; then
    error "Queue size must be a positive integer."
    exit 10
fi

# Main execution
START_TIME=$(date +%s)

info "OS Final Project — Financial Tick Data Pipeline"
info "Input  : $INPUT_DIR"
info "Output : $OUTPUT_DIR"
info "Threads: $NUM_THREADS  |  Queue: $QUEUE_SIZE"
echo ""

check_dependencies
check_input_dir
build_project

# create output and logs directories
mkdir -p "$OUTPUT_DIR" logs

info "Launching dispatcher..."
./dispatcher \
    -i "$INPUT_DIR" \
    -o "$OUTPUT_DIR" \
    -n "$NUM_THREADS" \
    -q "$QUEUE_SIZE" \
    -f /tmp/os_project_fifo \
    -s /os_project_shm \
    -e /os_project_sem_ready &

DISPATCHER_PID=$!
echo "$DISPATCHER_PID" > "$PID_FILE"
info "Dispatcher running with PID=$DISPATCHER_PID"

# wait for dispatcher to finish
wait "$DISPATCHER_PID"
DISPATCHER_EXIT=$?

rm -f "$PID_FILE"

# report exit status
case "$DISPATCHER_EXIT" in
    0)   info  "Pipeline completed successfully." ;;
    130) warn  "Pipeline interrupted by Ctrl-C (clean shutdown)." ;;
    143) warn  "Pipeline terminated by SIGTERM (clean shutdown)." ;;
    30)  error "A child process died unexpectedly." ;;
    40)  error "I/O error during pipeline execution." ;;
    *)   warn  "Pipeline exited with code $DISPATCHER_EXIT." ;;
esac

exit "$DISPATCHER_EXIT"

#!/bin/bash
# run_submit.sh

API_BASE="https://gromacs-tuner.dyn.cloud.e-infra.cz/api"
AUTH="admin:strong-secret-here"

# Usage info
usage() {
  echo "Usage: $0 [--tpr tprfile] [--extra-args args]"
  exit 1
}

if [ "$1" = "--tpr" ] && [ -n "$2" ]; then
  FILE="$2"
  if [ ! -f "$FILE" ]; then
    echo "Error: File '$FILE' not found."
    exit 1
  fi
  # Allow extra arguments to be passed after the tprfile
  shift 2
  EXTRA_ARGS=""
  if [ $# -gt 0 ]; then
    EXTRA_ARGS="$*"
  fi
  echo "Submitting $FILE to $API_BASE/tuner_runs..."
  if [ -n "$EXTRA_ARGS" ]; then
    response=$(curl -s -u "$AUTH" -X POST \
      -F "file=@${FILE}" \
      -F "extra_args=${EXTRA_ARGS}" \
      "$API_BASE/tuner_runs")
  else
    response=$(curl -s -u "$AUTH" -X POST -F "file=@${FILE}" "$API_BASE/tuner_runs")
  fi
  echo "Tuning Submission Response:"
  echo "$response"
  exit 0

else
  usage
fi

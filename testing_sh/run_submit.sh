#!/bin/bash
# run_submit.sh

API_BASE="https://gromacs-tuner.dyn.cloud.e-infra.cz/api"
AUTH="admin:strong-secret-here"

# Usage info
usage() {
  echo "Usage: $0 [--replica zipfile] [--plumed zipfile] [--tpr tprfile]"
  exit 1
}

if [ "$1" = "--replica" ] && [ -n "$2" ]; then
  ZIPFILE="$2"
  if [ ! -f "$ZIPFILE" ]; then
    echo "Error: File '$ZIPFILE' not found."
    exit 1
  fi
  echo "Submitting replica exchange ZIP to $API_BASE/replica_exchange..."
  response=$(curl -s -u "$AUTH" -X POST -F "file=@${ZIPFILE}" "$API_BASE/replica_exchange")
  echo "Replica Exchange Submission Response:"
  echo "$response"
  exit 0

elif [ "$1" = "--plumed" ] && [ -n "$2" ]; then
  ZIPFILE="$2"
  if [ ! -f "$ZIPFILE" ]; then
    echo "Error: File '$ZIPFILE' not found."
    exit 1
  fi
  # Allow extra arguments to be passed after the zipfile
  shift 2
  EXTRA_ARGS="-deffnm md -plumed plumed.dat"
  if [ $# -gt 0 ]; then
    EXTRA_ARGS="$EXTRA_ARGS $*"
  fi
  echo "Submitting plumed job ZIP to $API_BASE/custom_run..."
  response=$(curl -s -u "$AUTH" -X POST \
    -F "file=@${ZIPFILE}" \
    -F "extra_args=${EXTRA_ARGS}" \
    "$API_BASE/custom_run")
  echo "Plumed Submission Response:"
  echo "$response"
  exit 0

elif [ "$1" = "--tpr" ] && [ -n "$2" ]; then
  FILE="$2"
  if [ ! -f "$FILE" ]; then
    echo "Error: File '$FILE' not found."
    exit 1
  fi
  echo "Submitting $FILE to $API_BASE/tuner_runs..."
  response=$(curl -s -u "$AUTH" -X POST -F "file=@${FILE}" "$API_BASE/tuner_runs")
  echo "Tuning Submission Response:"
  echo "$response"
  exit 0

else
  usage
fi
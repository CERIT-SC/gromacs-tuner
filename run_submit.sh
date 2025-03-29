#!/bin/bash
# run_submit.sh

API_URL="https://gromacs-tuner.dyn.cloud.e-infra.cz/api/tuner_runs"
FILE="md.tpr"

if [ ! -f "$FILE" ]; then
  echo "Error: File '$FILE' not found."
  exit 1
fi

echo "Submitting $FILE to $API_URL..."
response=$(curl -s -X POST -F "file=@${FILE}" "$API_URL")
echo "Submission Response:"
echo "$response"
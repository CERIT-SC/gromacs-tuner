#!/bin/bash
# poll_status.sh

if [ -z "$1" ]; then
  echo "Usage: $0 <tuner_run_id>"
  exit 1
fi

JOB_ID="$1"
API_URL="https://gromacs-tuner.dyn.cloud.e-infra.cz/api/tuner_runs/${JOB_ID}/status"

echo "Polling status for job: $JOB_ID"

while true; do
  response=$(curl -s "$API_URL")
  echo "Response: $response"

  status=$(echo "$response" | jq -r '.status')
  if [ "$status" == "COMPLETED" ]; then
    echo "Job $JOB_ID completed."
    echo "Final results:"
    echo "$response" | jq
    break
  else
    echo "Job $JOB_ID still running. Waiting 30 seconds..."
    sleep 30
  fi
done
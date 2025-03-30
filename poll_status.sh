#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <tuner_run_id>"
  exit 1
fi

JOB_ID="$1"
API_URL="https://gromacs-tuner.dyn.cloud.e-infra.cz/api/tuner_runs/${JOB_ID}/status"

BOLD="\033[1m"
RESET="\033[0m"
GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
BLUE="\033[34m"

echo ""
echo -e "${BOLD}Polling status for job:${RESET} $JOB_ID"

while true; do
  response=$(curl -s "$API_URL")

  if [ -z "$response" ]; then
    echo -e "${RED}No response from server. Exiting.${RESET}"
    exit 1
  fi

  summary=$(echo "$response" | jq -r '.summary')
  cluster=$(echo "$response" | jq -r '.cluster_resources')

  running=$(echo "$summary" | jq -r '.RUNNING // 0')
  pending=$(echo "$summary" | jq -r '.PENDING // 0')
  terminated=$(echo "$summary" | jq -r '.TERMINATED // 0')
  error=$(echo "$summary" | jq -r '.ERROR // 0')

  echo ""
  echo -e "${BOLD}Job Summary:${RESET} RUNNING: ${YELLOW}${running}${RESET}, PENDING: ${BLUE}${pending}${RESET}, TERMINATED: ${GREEN}${terminated}${RESET}, ERROR: ${RED}${error}${RESET}"
  echo -e "${BOLD}Cluster Resources:${RESET} $cluster"

  echo ""
  echo -e "${BOLD}Trial Table:${RESET}"

  headers=$(echo "$response" | jq -r '[.trials[] | keys_unsorted] | add | unique | map(select(. != "id" and . != "tpr_path" and . != "status"))')
  header_list=$(echo "$headers" | jq -r '.[]' | sed 's/[[:space:]]\+$//')

  display_headers=()
  for key in $header_list; do
    if [ "$key" = "performance" ]; then
      display_headers+=("perf")
    else
      display_headers+=("$key")
    fi
  done

  header_format="│ %-20s │ %-10s"
  separator="├──────────────────────┼────────────"
  for key in $header_list; do
    header_format+=" │ %-10s"
    separator+="┼────────────"
  done
  header_format+=" │\n"
  separator+="┤"

  header_line="┌──────────────────────┬────────────"
  for key in $header_list; do
    header_line+="┬────────────"
  done
  header_line+="┐"

  echo "$header_line"
  printf "$header_format" "Trial ID" "Status" "${display_headers[@]}"
  echo "$separator"

  echo "$response" | jq -c '.trials[]' | while read -r trial; do
    id=$(echo "$trial" | jq -r '.id')
    status=$(echo "$trial" | jq -r '.status')
    color="$RESET"
    [[ "$status" == "RUNNING" ]] && color="$YELLOW"
    [[ "$status" == "TERMINATED" ]] && color="$GREEN"
    [[ "$status" == "ERROR" ]] && color="$RED"

    values=()
    for key in $header_list; do
      value=$(echo "$trial" | jq -r --arg k "$key" '.[$k] // "N/A"')
      values+=("$value")
    done

    printf "${color}│ %-20s │ %-10s" "$id" "$status"
    for val in "${values[@]}"; do
      printf " │ %-10s" "$val"
    done
    printf " │${RESET}\n"
  done

  footer="└──────────────────────┴────────────"
  for key in $header_list; do
    footer+="┴────────────"
  done
  footer+="┘"
  echo "$footer"

  if [[ "$running" == "0" && "$pending" == "0" ]]; then
    echo ""
    echo -e "${GREEN}${BOLD}Job $JOB_ID completed.${RESET}"
    break
  fi

  echo ""
  echo "Waiting 30 seconds before next check..."
  sleep 30
done
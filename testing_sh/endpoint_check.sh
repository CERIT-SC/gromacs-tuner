#!/bin/bash

BASE_URL="https://gromacs-tuner.dyn.cloud.e-infra.cz/api"
AUTH="admin:strong-secret-here"
JOB_ID="e9573405-abab-46e5-ba17-2c2fa7d6f8e6"

curl -s -u $AUTH "$BASE_URL/health" | jq .
curl -s -u $AUTH "$BASE_URL/tuner_runs/$JOB_ID/status" | jq .
curl -s -u $AUTH -X DELETE "$BASE_URL/tuner_runs/$JOB_ID" | jq .

#!/bin/bash
# trigger-data-update.sh - Triggers update of data sources.

set -o nounset
set -o errexit

CMD=$0

# Checks command-line arguments, sets variables, etc.
prepare () {

  if [[ -z ${GITHUB_TOKEN:-} ]]; then
    echo "Error: GITHUB_TOKEN must be set to a personal access token. See:"
    echo "https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line"
    exit 1
  fi
}

execute () {
  curl -H "Authorization: token $GITHUB_TOKEN" \
      --request POST \
      --data "{\"event_type\": \"update-source-data\" }" \
      https://api.github.com/repos/covid-projections/covid-data-public/dispatches

  echo "Data sources update requested. Go to https://github.com/covid-projections/covid-data-public/actions to monitor progress."
}

prepare "$@"
execute

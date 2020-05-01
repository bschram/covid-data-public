#!/bin/bash
set -o nounset
set -o errexit

python scripts/update.py
python scripts/update_covid_tracking_data.py
python scripts/update_covid_care_map.py
python scripts/update_interventions_naco.py

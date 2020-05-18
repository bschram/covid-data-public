#!/bin/bash
set -o nounset
set -o errexit

python scripts/update.py
python scripts/update_covid_tracking_data.py
python scripts/update_covid_care_map.py
python scripts/update_interventions_naco.py
python scripts/update_nha_hospitalization_county.py
python scripts/update_nytimes_data.py
python scripts/update_test_and_trace.py
python scripts/update_state_of_kentucky.py

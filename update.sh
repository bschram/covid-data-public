#!/bin/bash
set -o nounset
set -o errexit

python scripts/update.py
python scripts/update_covid_tracking_data.py
python scripts/update_covid_care_map.py
python scripts/update_nha_hospitalization_county.py
python scripts/update_nytimes_data.py
python scripts/update_test_and_trace.py
python scripts/update_state_of_kentucky.py
python scripts/update_texas_tsa_hospitalizations.py
python scripts/update_texas_fips_hospitalizations.py
python scripts/update_aws_lake.py --replace_local_mirror
# Needs to run after scripts/update.py call to update_cds_data returns.
python scripts/update_covid_data_scraper.py

python scripts/update_cmdc.py

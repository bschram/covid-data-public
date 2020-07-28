#!/bin/bash
set -o nounset
set -o errexit

python scripts/update.py
python scripts/update_covid_tracking_data.py
python scripts/update_covid_care_map.py
python scripts/update_nha_hospitalization_county.py
python scripts/update_nytimes_data.py
python scripts/update_test_and_trace.py
python scripts/update_texas_tsa_hospitalizations.py
python scripts/update_texas_fips_hospitalizations.py
python scripts/update_covid_data_scraper.py

python scripts/update_cmdc.py
# AWS Lake seems to be hanging the build right now.
# python scripts/update_aws_lake.py --replace-local-mirror --cleanup-local-mirror

'''
Fetches data from naco.org and transforms it to produce county-level intervention data.
The format is {
  "<fips code>": "<intervention>"
}

where intervention is one of {shelter_in_place, social_distancing, limited_action}
'''

from datetime import datetime, timezone
import json
import os
import pytz
import requests

NACO_URL = 'https://ce.naco.org/python/data?dbset=COVID_Table&dbind=County_Declaration_Type3%7CCounty_Emergency_Declaration%7CCounty_Shelter_In_Place_Policy%7CCounty_Business_Closure_Policy%7CPopulation_total%7CPopulation_over_65_pct&dbyear=2020'

def fetch_naco():
  resp = requests.get(NACO_URL)
  return resp.json()

def transform_json(columns, data):
  fips_index = columns.index('FIPS')
  shelter_index = columns.index('County_Shelter_In_Place_Policy')
  county_emergency = columns.index('County_Emergency_Declaration')
  county_closure = columns.index('County_Business_Closure_Policy')

  def row_intervention(row) -> str:
    if row[shelter_index] != None:
      return 'shelter_in_place'
    elif row[county_emergency] != None:
      return 'social_distancing'
    elif row[county_closure] != None:
      return 'social_distancing'
    return 'limited_action'

  return {row[fips_index]: row_intervention(row) for row in data}

def stamp():
    #  String of the current date and time.
    #  So that we're consistent about how we mark these
    pacific = pytz.timezone('US/Pacific')
    d = datetime.now(pacific)
    return d.strftime('%A %b %d %I:%M:%S %p %Z')

if __name__ == '__main__':
  # Quick hack for development w/o hitting the API every time:
  # if an argument is passed, used that as the data source rather than the URL
  import sys
  last_arg = sys.argv[-1]
  if last_arg != __file__:
    fname = last_arg
    with open(fname, 'r') as f:
      data = json.load(f)
  else:
    data = fetch_naco()
  #now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S %Z")
  now = str(datetime.now(timezone.utc))
  fips_interventions = transform_json(data['columns'], data['data'])
  output_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'data', 'interventions-naco'))
  os.makedirs(output_dir, exist_ok=True)
  json_file = os.path.join(output_dir, 'county-interventions.json')
  with open(json_file, 'w') as f:
    json.dump(fips_interventions, f, indent=2, sort_keys=True)
  version_file = os.path.join(output_dir, 'version.txt')
  with open(version_file, 'w') as f:
    f.write('Updated on {}'.format(stamp()))
  print(f'wrote {len(fips_interventions)} county interventions to {json_file} at {stamp()}')

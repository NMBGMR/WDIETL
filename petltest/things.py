# ===============================================================================
# Copyright 2020 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
import json

import petl
import requests

from petltest import get_nm_aquifier_connection, GOST_URL


def extract_location():
    sql = 'select Locationid, PointID, SiteNames, LatitudeDD, LongitudeDD  from dbo.Location' \
          ' where PublicRelease=1 and LatitudeDD is not null'
    table = petl.fromdb(get_nm_aquifier_connection(), sql)
    return table


def make_thing(ld):
    return {'name': ld['PointID'],
            'description': 'Water Well',
            'properties': {},
            'Locations': [{'name': ld['SiteNames'] or 'No Name',
                           'description': 'No Description',
                           'encodingType': 'application/vnd.geo+json',
                           'location': {'type': 'Point',
                                        'coordinates': [ld['LongitudeDD'],
                                                        ld['LatitudeDD']]}
                           }]
            }


def post_thing(thing):
    resp = requests.post(f'{GOST_URL}/Things', json=thing)

    if resp.status_code != 201:
        print(thing, resp.json())
    else:
        return resp.json()['@iot.id']


def load_things(table):
    # create a point_id: @iot.thing.id mapping for convienence
    mapping = {}
    for t in petl.dicts(table):
        thing = make_thing(t)
        tid = post_thing(thing)
        if tid is not None:
            print(f"loaded {t['Locationid']}, {t['PointID']} as {tid}")
            mapping[str(t['Locationid'])] = tid

    return mapping


def dump_thing_mapping(obj):
    with open('thing_mapping.json', 'w') as wfile:
        json.dump(obj, wfile)


def etl_things():
    location_table = extract_location()
    thing_mapping = load_things(location_table)
    dump_thing_mapping(thing_mapping)

# ============= EOF =============================================

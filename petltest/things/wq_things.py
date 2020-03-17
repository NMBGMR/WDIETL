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

from petltest import get_nm_quality_connection, GOST_URL, post_item, get_item_by_name
from petltest.things import dump_thing_mapping


def extract_location():
    sql = '''select POINT_ID, WQ_Arsenic.Latitude, WQ_Arsenic.Longitude, SiteNames from dbo.WQ_Arsenic
join NM_Aquifer.dbo.Location on NM_Aquifer.dbo.Location.PointID = dbo.WQ_Arsenic.POINT_ID
where PublicRelease=1'''

    table = petl.fromdb(get_nm_quality_connection(), sql)
    return table


def make_thing(ld):
    return {'name': ld['POINT_ID'],
            'description': 'Water Chemistry Well',
            'properties': {},
            'Locations': [{'name': ld['SiteNames'] or 'No Name',
                           'description': 'No Description',
                           'encodingType': 'application/vnd.geo+json',
                           'location': {'type': 'Point',
                                        'coordinates': [ld[f'Longitude'],
                                                        ld[f'Latitude']]}
                           }]
            }


def load_things(table):
    # create a point_id: @iot.thing.id mapping for convienence
    mapping = {}
    for l in petl.dicts(table):
        thing = make_thing(l)
        tid = get_item_by_name('Things', thing['name'])
        if tid is not None:
            print(f"Thing: name={thing['name']} already exists skipping")
        else:
            tid = post_item('Things', thing)

        if tid is not None:
            print(f"loaded {l['POINT_ID']} as {tid}")
            mapping[str(l['POINT_ID'])] = tid

    return mapping


def etl_things():
    location_table = extract_location()
    thing_mapping = load_things(location_table)
    dump_thing_mapping(thing_mapping, 'wq_things_mapping')

# ============= EOF =============================================

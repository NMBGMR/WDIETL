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

from petltest import nm_aquifier_connection, GOST_URL, post_item, get_item_by_name
from petltest.things import dump_thing_mapping


def extract_location():
    sql = 'select Locationid, PointID, SiteNames, LatitudeDD, LongitudeDD  from dbo.Location' \
          ' where PublicRelease=1 and LatitudeDD is not null'
    table = petl.fromdb(nm_aquifier_connection(), sql)
    return table


# @todo: refactor following wq_things
# def make_thing(ld):
#     return {'name': ld['PointID'],
#             'description': 'Water Well',
#             'properties': {},
#             'Locations': [{'name': ld['SiteNames'] or 'No Name',
#                            'description': 'No Description',
#                            'encodingType': 'application/vnd.geo+json',
#                            'location': {'type': 'Point',
#                                         'coordinates': [ld['LongitudeDD'],
#                                                         ld['LatitudeDD']]}
#                            }]
#             }
#
#
# def load_things(table):
#     # create a point_id: @iot.thing.id mapping for convienence
#     mapping = {}
#     for l in petl.dicts(table):
#         thing = make_thing(l)
#         if get_item_by_name('Things', thing['name']):
#             print(f"Thing: name={thing['name']} already exists skipping")
#             continue
#
#         tid = post_item('Things', thing)
#         if tid is not None:
#             print(f"loaded {l['Locationid']}, {l['PointID']} as {tid}")
#             mapping[str(l['Locationid'])] = tid
#
#     return mapping
#
#
# def etl_things():
#     location_table = extract_location()
#     thing_mapping = load_things(location_table)
#     dump_thing_mapping(thing_mapping, 'wl_thing_mapping')

# ============= EOF =============================================

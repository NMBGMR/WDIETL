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

from petltest import nm_quality_connection, GOST_URL, post_item, get_item_by_name, get_items, ask
from petltest.models.wq_models import ARSENIC, CA

THING_NAME = 'WaterChemistryAnalysis'


def extract_location(table, offset, n):
    sql = f'''select POINT_ID, WQ_{table}.Latitude, WQ_{table}.Longitude, SiteNames, WellDepth from dbo.WQ_{table}
join NM_Aquifer.dbo.Location on NM_Aquifer.dbo.Location.PointID = dbo.WQ_{table}.POINT_ID
where PublicRelease=1
order by POINT_ID offset %d rows fetch first %d rows only'''

    table = petl.fromdb(nm_quality_connection(), sql, (offset, n))
    return table


def make_thing(ld, location_id):
    return {'name': THING_NAME,
            'description': 'Water Chemistry Analysis of a Well',
            'properties': {'@nmbgmr.point_id': ld['POINT_ID']},
            'Locations': [{'@iot.id': location_id}]}


def make_location(ld):
    return {'name': ld['POINT_ID'],
            'description': ld['SiteNames'] or 'No Description',
            'encodingType': 'application/vnd.geo+json',
            'location': {'type': 'Point',
                         'coordinates': [ld[f'Longitude'],
                                         ld[f'Latitude']]}}


def load_things(dbtable, model, observation_hook=None):
    for record in petl.dicts(dbtable):
        post_thing(record, model, observation_hook)


def get_existing_thing(lid):
    items = get_items(f'Locations({lid})?$expand=Things')
    for i in items:
        try:
            for ti in i['Things']:
                if ti['name'] == THING_NAME:
                    return ti['@iot.id']
        except KeyError:
            continue


def post_thing(record, model, observation_hook=None):
    location = make_location(record)
    location_id = get_item_by_name('Locations', location['name'])
    if location_id is None:
        location_id = post_item('Locations', location)
    else:
        print(f'location {location_id} already exists')

    tid = get_existing_thing(location_id)
    # dont add this thing if already exists at this location
    if tid is None:
        thing = make_thing(record, location_id)
        tid = post_item('Things', thing)
    else:
        print(f'thing {tid} already exists')

    if tid is not None:
        if observation_hook:
            observation_hook(tids=({'@iot.id': tid,
                                    '@nmbgmr.point_id': record['POINT_ID']},),
                             models=(model,))


def etl_things(observation_hook=None):
    offset = 0
    n = 1

    while 1:
        for m in (ARSENIC, CA):
            print(f'Importing water chem model: {m.name}')
            location_table = extract_location(m.name, offset, n)
            load_things(location_table, m, observation_hook)

        offset += n

        if not ask('Continue to next location batch y/[n]'):
            return
# ============= EOF =============================================

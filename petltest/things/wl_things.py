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

from petltest import nm_aquifier_connection, GOST_URL, post_item, get_item_by_name, ask
from petltest.models.wl_models import WATER_HEAD
from petltest.things.things import BaseThings


class WaterLevelPressureThings(BaseThings):
    __thing_name__ = 'WaterLevelPressure'

    def extract(self, model):
        sql = 'select Locationid, PointID, SiteNames, LatitudeDD, LongitudeDD  from dbo.Location' \
              ' where PublicRelease=1 and LatitudeDD is not null' \
              'order by PointID offset %d rows fetch %d rows only'
        table = petl.fromdb(nm_aquifier_connection(), sql, (self.offset, self.n))
        return table

    def _make_location(self, record):
        return {'name': record['SiteNames'] or 'No Name',
                'description': 'No Description',
                'encodingType': 'application/vnd.geo+json',
                'location': {'type': 'Point',
                             'coordinates': [record['LongitudeDD'],
                                             record['LatitudeDD']]}}

    def _make_thing(self, record, location_id):
        return {'name': self.__thing_name__,
                'description': 'Water Well',
                'properties': {},
                'Locations': [{'@iot.id': location_id}]}


# class WaterLevelAcousticThings(BaseThings):
#     pass

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
# def load_things(dbtable, model, observation_hook=None):
#     for record in petl.dicts(dtable):
#         post_thing(record, model, observation_hook)
#
#
# def post_thing(record, model, observation_hook):
#     location = make_location(record)
#     location_id = get_item_by_name('Location', location['name'])
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


# ============= EOF =============================================

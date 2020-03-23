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
from petltest.models.wl_models import WATER_HEAD, DEPTH_TO_WATER
from petltest.things.things import BaseThings


class WaterLevelPressureThings(BaseThings):
    __thing_name__ = 'WaterLevelPressure'
    __models__ = (WATER_HEAD, DEPTH_TO_WATER)
    id = 'waterlevelpressure'

    def extract(self, model):
        sql = '''select PointID, SiteNames, LatitudeDD, LongitudeDD from dbo.Location
              where PublicRelease=1 and LatitudeDD is not null
              order by PointID offset %d rows fetch first %d rows only'''

        table = petl.fromdb(nm_aquifier_connection(), sql, (self.offset, self.n))
        return table

    def _has_observations(self, record):
        sql = '''select count(PointID) from dbo.WaterLevelsContinuous_Pressure
        where PointID=%s'''
        pid = record['PointID']
        table = petl.fromdb(nm_aquifier_connection(), sql, (pid,))
        nobs = petl.values(table, '')[0]
        print(f'{pid} has nobs={nobs}')
        return bool(nobs)

    def _make_location(self, record):
        return {'name': record['PointID'],
                'description': record['SiteNames'] or 'No Description',
                'encodingType': 'application/vnd.geo+json',
                'location': {'type': 'Point',
                             'coordinates': [record['LongitudeDD'],
                                             record['LatitudeDD']]}}

    def _make_thing(self, record, location_id):
        return {'name': self.__thing_name__,
                'description': 'Water Well',
                'properties': {'@nmbgmr.point_id': record['PointID']},
                'Locations': [{'@iot.id': location_id}]}

    def _make_tids(self, tid, record):
        return {'@iot.id': tid,
                '@nmbgmr.point_id': record['PointID']},

# class WaterLevelAcousticThings(BaseThings):
#     pass

# ============= EOF =============================================
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
# def find_test_waterlevels_pointid(self):
    #     self.offset = 500
    #     self.n = 10
    #     while 1:
    #         dbtable = self.extract(self.__models__[0])
    #         for record in petl.dicts(dbtable):
    #             sql = '''select count(PointID) from dbo.WaterLevelsContinuous_Pressure
    #             where PointID=%d'''
    #             table = petl.fromdb(nm_aquifier_connection(), sql, (record['PointID'],))
    #
    #             print(record['PointID'])
    #             print(table)
    #
    #         self.offset += self.n



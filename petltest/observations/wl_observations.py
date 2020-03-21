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
import pytz
import requests

from petltest import nm_aquifier_connection, GOST_URL, post_item, get_item_by_name, make_id
from petltest.datastreams import add_datastream
from petltest.models.wl_models import WATER_HEAD, WATER_HEAD_ADJUSTED, WATER_TEMPERATURE, WATER_CONDUCTIVITY, \
    DEPTH_TO_WATER, AIR_TEMPERATURE
from petltest.observations import get_datastream, MT_TIMEZONE
from petltest.observed_properties import add_observed_property
from petltest.sensors import add_sensor


# @todo: refactor to  wq style
# def extract_waterlevels_continuous(sensor, lid):
#     if sensor == 'Pressure':
#         sql = f'select * from dbo.WaterLevelsContinuous_Pressure ' \
#               f'join dbo.Location on dbo.Location.PointID = dbo.WaterLevelsContinuous_Pressure.PointID ' \
#               f'where Locationid=%s and QCed=1 and PublicRelease=1'
#     elif sensor == 'Acoustic':
#         sql = f'select * from dbo.WaterLevelsContinuous_Acoustic ' \
#               f'join dbo.Location on dbo.Location.PointID = dbo.WaterLevelsContinuous_Acoustic.PointID ' \
#               f'where Locationid=%s and PublicRelease=1'
#
#     table = petl.fromdb(nm_aquifier_connection(), sql, lid)
#
#     return petl.sort(table, 'DateMeasured')
#
#
# def add_observations(datastream_id, wt, col, ntotal):
#     cnt = 0
#     for wti in petl.dicts(wt):
#         # make the observation
#
#         if cnt and not cnt % 100:
#             print(f'adding observation {cnt}/{ntotal}')
#
#         t = MT_TIMEZONE.localize(wti['DateMeasured'])
#         v = wti[col]
#         if v is not None:
#             payload = {'phenomenonTime': t.isoformat(timespec='milliseconds'),
#                        'resultTime': t.isoformat(timespec='milliseconds'),
#                        'result': v,
#                        'Datastream': make_id(datastream_id)
#                        }
#             post_item(f'Observations', payload)
#             cnt += 1
#
#
# def etl_wl_observations():
#     add_obs_to_existing_ds = False
#
#     with open('thing_mapping.json', 'r') as rfile:
#         obj = json.load(rfile)
#
#     # add sensors
#     sensors = {'Pressure': add_sensor('WaterLevel_Pressure',
#                                       {'description': 'Diver Pressure Sensor',
#                                        'encodingType': 'application/pdf',
#                                        'metadata': 'foo'}),
#                'Acoustic': add_sensor('WaterLevel_Acoustic',
#                                       {'description': 'Acoustic Sensor',
#                                        'encodingType': 'application/pdf',
#                                        'metadata': 'bar'})}
#     #
#     tags = [WATER_HEAD,
#             WATER_HEAD_ADJUSTED,
#             WATER_TEMPERATURE,
#             WATER_CONDUCTIVITY,
#             DEPTH_TO_WATER,
#             AIR_TEMPERATURE
#             ]
#
#     observed_properties = {}
#     # add the observed properties
#     for m in tags:
#         observed_properties[m.name] = add_observed_property(m.name, m.observed_property_payload)
#
#     for i, (k, thing_id) in enumerate(obj.items()):
#         if i > 40:
#             break
#
#         print(i, k, thing_id)
#         for sensor in ('Pressure', 'Acoustic'):
#             # are there waterlevels in the database for this Thing/Locationid
#             wt = extract_waterlevels_continuous(sensor, k)
#             nrows = petl.nrows(wt)
#             if nrows:
#                 print(f'Add {sensor} observations. count={nrows}')
#                 header = petl.header(wt)
#
#                 # add datastreams to thing
#                 for m in tags:
#                     if m.mapped_column in header:
#                         print(f'Adding datastream {observed_properties[m.name]}, {sensors[sensor]}')
#
#                         ds_id = get_datastream(thing_id, m.datastream_payload['name'])
#                         add_obs = True
#                         if not ds_id:
#                             ds_id = add_datastream(thing_id, observed_properties[m.name], sensors[sensor],
#                                                    m.datastream_payload)
#                         else:
#                             add_obs = add_obs_to_existing_ds
#
#                         if ds_id and add_obs:
#                             # add observations to datastream
#                             add_observations(ds_id, wt, m.mapped_column)

# ============= EOF =============================================

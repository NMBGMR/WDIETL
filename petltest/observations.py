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
from petltest.datastreams import add_datastream
from petltest.observed_properties import add_observed_property
from petltest.sensors import add_sensor


def extract_waterlevels_continuous(sensor, lid):
    if sensor=='Pressure':
        sql = f'select * from dbo.WaterLevelsContinuous_Pressure ' \
              f'join dbo.Location on dbo.Location.PointID = dbo.WaterLevelsContinuous_Pressure.PointID ' \
              f'where Locationid=%s'
    elif sensor == 'Acoustic':
        sql = f'select * from dbo.WaterLevelsContinuous_Acoustic ' \
              f'join dbo.Location on dbo.Location.PointID = dbo.WaterLevelsContinuous_Acoustic.PointID ' \
              f'where Locationid=%s'

    table = petl.fromdb(get_nm_aquifier_connection(), sql, lid)
    return table


def make_observation(wti):
    pass


def add_observations(datastream_id, wt):
    for wti in petl.dicts(wt):
        obs = make_observation(wti)
        requests.post(f'{GOST_URL}/Datastreams({datastream_id})/Observations', json=obs)


def etl_observations():
    with open('thing_mapping.json', 'r') as rfile:
        obj = json.load(rfile)

    # add sensors
    sensors = {'Pressure': add_sensor('Pressure'),
               'Acoustic': add_sensor('Acoustic')}

    #  tag, column name
    tags = [('WaterHead', 'WaterHead',
             {'description': 'Water Head above sensor',
              'definition': 'No Definition'}),
            ("WaterHeadAdjusted", 'WaterHeadAdjusted',
             {'description': 'Adjusted Water Head above sensor',
              'definition': 'No Definition'}),
            ('WaterTemperature', 'TemperatureWater',
             {'description': 'Temperature of the water in the well',
              'definition': 'No Definition'}),
            ('WaterConductivity', 'CONDDL',
             {'description': 'Electrical conductivity of the water',
              'definition': 'No Definition'}),
            ('DepthToWater', 'DepthToWaterBGS',
             {'description': 'Depth to water below the ground surface',
              'definition': 'No Definition'}),
            ('AirTemperature', 'TemperatureAir',
             {'description': 'Air temperature at the surface',
              'definition': 'No Definition'})]

    observed_properties = {}
    # add the observed properties
    for t, _, op in tags:
        observed_properties[t] = add_observed_property(t, op)

    for k, thing_id in obj.items():
        print(k, thing_id)
        for sensor in ('Pressure', 'Acoustic'):
            # are there waterlevels in the database for this Thing/Locationid
            wt = extract_waterlevels_continuous(sensor, k)
            if petl.nrows(wt):
                header = petl.header(wt)

                # add datastreams to thing
                for t, col, _ in tags:
                    if col in header:
                        ds_id = add_datastream(thing_id, observed_properties[t], sensors[sensor])

                        # add observations to datastream
                        add_observations(ds_id, wt, col)

# ============= EOF =============================================

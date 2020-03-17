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

from petltest import get_nm_quality_connection, make_id, post_item
from petltest.datastreams import add_datastream
from petltest.import_models import ARSENIC, CA
from petltest.observations import get_datastream, MT_TIMEZONE
from petltest.observed_properties import add_observed_property
from petltest.sensors import add_sensor


def extract_species(point_id,  table, column):
    sql = f'''select POINT_ID, CollectionDate, 
{column}, WQ_{table}.Latitude, WQ_{table}.Longitude, {column}_Symbol, SiteNames 
from dbo.WQ_{table} 
join NM_Aquifer.dbo.Location on NM_Aquifer.dbo.Location.PointID = dbo.WQ_{table}.POINT_ID
where PublicRelease=1 and POINT_ID=%s'''

    table = petl.fromdb(get_nm_quality_connection(), sql, point_id)
    return table


def add_observations(datastream_id, wt, col):
    for i, wti in enumerate(petl.dicts(wt)):
        # make the observation
        if i and not i % 100:
            print(f'adding observation {i}')

        t = MT_TIMEZONE.localize(wti['CollectionDate'])
        v = wti[col]

        payload = {'phenomenonTime': t.isoformat(timespec='milliseconds'),
                   'resultTime': t.isoformat(timespec='milliseconds'),
                   'result': v,
                   'Datastream': make_id(datastream_id)
                   }
        post_item(f'Observations', payload)


def etl_wq_observations():
    with open('wq_things_mapping.json', 'r') as rfile:
        obj = json.load(rfile)

    sensor_id = add_sensor('WaterChemistry',
                           {'description': 'NMBGMR WaterChemistry Lab',
                            'encodingType': 'application/pdf',
                            'metadata': 'foo'})

    tags = [ARSENIC, CA,]

    observed_properties = {}
    # add the observed properties
    for m in tags:
        observed_properties[m.name] = add_observed_property(m.name, m.observed_property_payload)

    for i, (point_id, thing_id) in enumerate(obj.items()):
        if i > 40:
            break
        print(point_id)
        # for table, column in (('Arsenic','Arsenic'),):
        for m in tags:
            wt = extract_species(point_id, m.name, m.mapped_column)
            nrows = petl.nrows(wt)
            if nrows:
                print(f'Add {m.name} observations. count={nrows}')
                print(wt)
                if not get_datastream(thing_id, m.datastream_payload['name']):
                    ds_id = add_datastream(thing_id, observed_properties[m.name], sensor_id,
                                           m.datastream_payload)

                    # add observations to datastream
                    add_observations(ds_id, wt, m.mapped_column)

# ============= EOF =============================================

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

from petltest import nm_quality_connection, make_id, post_item, thing_generator
from petltest.datastreams import add_datastream
from petltest.models.wq_models import ARSENIC, CA, CL, F, MG, NA, SO4, DEFAULT_MODELS
from petltest.observations import get_datastream, MT_TIMEZONE
from petltest.observations.observations import BaseObservations
from petltest.observed_properties import add_observed_property
from petltest.sensors import add_sensor


class WaterChemistryObservations(BaseObservations):
    __models__ = DEFAULT_MODELS
    __thing_name__ = 'WaterChemistryAnalysis'

    def _add_sensor(self):
        return add_sensor('WaterChemistry',
                          {'description': 'NMBGMR WaterChemistry Lab',
                           'encodingType': 'application/pdf',
                           'metadata': 'foo'})

    def _extract(self, point_id, model, skip):
        column = model.mapped_column
        table = model.name

        sql = f'''select POINT_ID, CollectionDate, {column},  {column}_Symbol 
        from dbo.WQ_{table} 
        join NM_Aquifer.dbo.Location on NM_Aquifer.dbo.Location.PointID = dbo.WQ_{table}.POINT_ID
        where PublicRelease=1 and POINT_ID=%s
        order by CollectionDate offset %d rows'''

        table = petl.fromdb(nm_quality_connection(), sql, (point_id, skip))
        return table

# ============= EOF =============================================

# def etl_wq_observations(tids=None, models=None):
#     sensor_id = add_sensor('WaterChemistry',
#                            {'description': 'NMBGMR WaterChemistry Lab',
#                             'encodingType': 'application/pdf',
#                             'metadata': 'foo'})
#
#     if models is None:
#         models = [ARSENIC, CA, CL, F, MG, NA, SO4]
#
#     observed_properties = {}
#     # add the observed properties
#     for m in models:
#         observed_properties[m.name] = add_observed_property(m.name, m.observed_property_payload)
#
#     # for i, (point_id, thing_id) in enumerate(obj.items()):
#     if tids is None:
#         tids = thing_generator('WaterChemistryAnalysis')
#     else:
#         if not isinstance(tids, (list, tuple)):
#             tids = (tids,)
#
#     for thing in tids:
#         thing_id = thing['@iot.id']
#         point_id = thing['@nmbgmr.point_id']
#         for m in models:
#             wt = extract_species(point_id, m.name, m.mapped_column)
#             nrows = petl.nrows(wt)
#             if nrows:
#                 print(f'Add {m.name} observations. count={nrows}')
#                 if not get_datastream(thing_id, m.datastream_payload['name']):
#                     ds_id = add_datastream(thing_id, observed_properties[m.name], sensor_id,
#                                            m.datastream_payload)
#
#                     # add observations to datastream
#                     add_observations(ds_id, wt, m.mapped_column)


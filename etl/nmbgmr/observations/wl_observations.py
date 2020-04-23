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

import petl

from etl.nmbgmr.connections import nm_aquifier_connection
from etl.nmbgmr.models.wl_models import WATER_HEAD, WATER_HEAD_ADJUSTED, WATER_TEMPERATURE, WATER_CONDUCTIVITY, \
    DEPTH_TO_WATER, AIR_TEMPERATURE
from etl.st.observations import BaseObservations


class WaterLevelPressureObservations(BaseObservations):
    __models__ = (WATER_HEAD, DEPTH_TO_WATER)

    def _add_sensor(self):
        return self._post_unique_item('Sensors',
                                      {'description': 'Diver Pressure Sensor',
                                       'encodingType': 'application/pdf',
                                       'metadata': 'foo',
                                       'name': 'WaterLevel_Pressure'})

    def _extract(self, thing, model, skip):
        point_id = thing['@nmbgmr.point_id']

        sql = f'''select DateMeasured, {model.mapped_column}
        from dbo.WaterLevelsContinuous_Pressure
        join dbo.Location on dbo.Location.PointID = dbo.WaterLevelsContinuous_Pressure.PointID
        where dbo.Location.PointID = %d and QCed=1 
        order by DateMeasured offset %d rows
        '''

        return petl.fromdb(nm_aquifier_connection(), sql, (point_id, skip))

# ============= EOF =============================================

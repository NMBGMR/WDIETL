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

from etl.nmbgmr.atoms.base import Atom
from etl.nmbgmr.connections import nm_quality_connection
from etl.nmbgmr.models.wq_models import ARSENIC, CA, DEFAULT_MODELS
from etl.nmbgmr.observations.wq_observations import WaterChemistryObservations


class WaterChemAtom(Atom):
    __thing_name__ = 'WaterChemistryAnalysis'
    __models__ = DEFAULT_MODELS
    __observation_klass = WaterChemistryObservations

    id = 'waterchem'

    def extract(self, model, record_id):
        table = model.name
        sql = f'''select POINT_ID, WQ_{table}.Latitude, WQ_{table}.Longitude, SiteNames, WellDepth from dbo.WQ_{table}
    join NM_Aquifer.dbo.Location on NM_Aquifer.dbo.Location.PointID = dbo.WQ_{table}.POINT_ID
    where PublicRelease=1 and POINT_ID = %d'''

        table = petl.fromdb(nm_quality_connection(), sql, record_id)
        return table

    def _has_observations(self, record):
        # since water chem table is easily joined with location table
        # observation included in extract
        return True

    def _make_tids(self, tid, record):
        return {'@iot.id': tid,
                '@nmbgmr.point_id': record['POINT_ID']},

    def _make_location(self, record):
        return {'name': record['POINT_ID'],
                'description': record['SiteNames'] or 'No Description',
                'encodingType': 'application/vnd.geo+json',
                'location': {'type': 'Point',
                             'coordinates': [record[f'Longitude'],
                                             record[f'Latitude']]}}

    def _make_thing(self, record, location_id):
        return {'name': self.__thing_name__,
                'description': 'Water Chemistry Analysis of a Well',
                'properties': {'@nmbgmr.point_id': record['POINT_ID']},
                'Locations': [{'@iot.id': location_id}]}

# ============= EOF =============================================

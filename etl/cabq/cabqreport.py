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
import yaml

from etl.cabq.models.wl_models import DEPTH_TO_WATER
from etl.cabq.observations import CABQObservations
from etl.st.atom import STAtom


class CABQReport(STAtom):
    """
    this class is used to read a report and upload to ST
    required inputs are an xls file with all the data non-normalized
    and a yaml file that describes vocabulary

    """
    __observation_klass__ = CABQObservations
    __thing_name__ = 'WaterLevels'

    def __init__(self, config, *args, **kw):
        super(CABQReport, self).__init__(config, *args, **kw)

        self._src_path = config['src_path']
        self._vocab = self._get_vocab(config['vocab_path'])
        self.observation.set_vocab(self._vocab)

    def etl(self, *args, **kw):
        table = petl.fromxlsx(self._src_path)

        model = DEPTH_TO_WATER
        self._update_model(model, self._vocab)

        # group table by sys_loc_code
        header = petl.header(table)
        for name, records in petl.rowgroupby(petl.sort(table, 'sys_loc_code'), 'sys_loc_code'):
            records = [dict(zip(header, record)) for record in records]
            record = records[0]
            location_id = self._post_location(record, model)
            thing_id = self._post_thing(record, model, location_id)

            print('---------------')
            print(f'len records {len(records)}')
            # self.add_package(record)
            self.observation.set_records(records)
            self.observation.etl(tids=self._make_tids(thing_id, record),
                                 models=(model,))

    # private
    def _update_model(self, model, yobj):
        model.mapped_column = yobj['observation']['result']
        model.timestamp_column = yobj['observation']['time']

    def _get_vocab(self, p):
        with open(p, 'r') as rfile:
            return yaml.load(rfile, Loader=yaml.Loader)

    # ATOM requirements
    def _make_location(self, record):
        loc = self._vocab['location']
        return {'name': record[loc['name']],
                'description': record[loc['description']] or 'No Description',
                'encodingType': 'application/vnd.geo+json',
                'location': {'type': 'Point',
                             'coordinates': [record[loc['longitude']],
                                             record[loc['latitude']]]}}

    def _make_thing(self, record, location_id):
        return {'name': self.__thing_name__,
                'description': 'Water Well',
                'properties': {},
                'Locations': [{'@iot.id': location_id}]}

    def _make_tids(self, tid, record):
        return {'@iot.id': tid},


# ============= EOF =============================================

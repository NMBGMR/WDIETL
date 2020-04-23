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
from datetime import datetime

from etl.st.observations import BaseObservations


class CABQObservations(BaseObservations):
    _vocab = None

    def _add_sensor(self, records=None):
        if records is not None:
            # make payload from first record

            record = records[0]
            payload = {'metadata': 'None',
                       'encodingType': 'application/pdf',
                       'description': record.get(self._vocab['sensor']['description'], 'No Description')
                                      or 'No Description',
                       'name': record.get(self._vocab['sensor']['name'], 'No Sensor')
                               or 'No Sensor'}

            return self._post_unique_item('Sensors', payload)

    def set_vocab(self, v):
        self._vocab = v

    # def _add_sensor(self):
    #     return self._post_unique_item('Sensors',
    #                                   {'description': 'manual dip measurement',
    #                                    'encodingType': 'application/pdf',
    #                                    'metadata': 'foo',
    #                                    'name': 'Dip'})

    # def _timestamp_extract(self, t):
    #     """
    #     convert string to datetime
    #     :param t:
    #     :return:
    #     """
    #
    #     return datetime.strptime(t, self._vocab['observation']['time_format'])
# ============= EOF =============================================

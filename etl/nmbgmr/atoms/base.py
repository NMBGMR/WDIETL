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
import requests

from etl.ckan_importer import CKANImporter
from etl.st.atom import STAtom
from etl.st.base import STBase


class Atom(STAtom):
    id = None
    _added = False

    def etl(self, record_id):
        for model in self.__models__:
            location_table = self.extract(model, record_id)
            nrows = petl.nrows(location_table)

            if nrows == 1:
                record = petl.dicts(location_table)[0]
                if self._has_observations(record):
                    self._added = True
                    location_id = self._post_location(record, model)
                    thing_id = self._post_thing(record, model, location_id)

                    self.add_package(record)
                    self.observation.etl(tids=self._make_tids(thing_id, record),
                                         models=(model,))
            else:
                print(f'multipe records found for given record_id. Skipping {record_id}')

        # while 1:
        #     self._read_persist()
        #     # self._added = False
        #     for m in self.__models__:
        #         print(f'Importing water level model: {m.name}')
        #         location_table = self.extract(m)
        #         self.load_locations_things(location_table, m)
        #
        #     self.offset += self.n
        #     self._write_persist()
        #     # if self._added:
        #     if not ask('Continue to next location batch y/[n]'):
        #         return

    # def delete_thing(self, tid):
    #     delete_item(f'/Things({tid})')
    #
    # def delete_location(self, tid):
    #     delete_item(f'/Locations({tid})')


# ============= EOF =============================================
#     def _read_persist(self):
#         if os.path.isfile(self._persistence_path):
#             with open(self._persistence_path, 'r') as rfile:
#                 yd = yaml.load(rfile)
#                 self.offset = yd.get('offset', 0)
#
#     def _write_persist(self):
#         with open(self._persistence_path, 'w') as wfile:
#             yaml.dump({'offset': self.offset}, wfile)
#
#     @property
#     def _persistence_path(self):
#         return f'{self.id}_persistence.yaml'

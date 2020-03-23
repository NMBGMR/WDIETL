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
import os

import petl
import yaml

from petltest import ask, post_item, get_items, get_item_by_name, delete_item


class BaseThings(object):
    offset = 0
    n = 1
    observation_hook = None
    __thing_name__ = None
    __models__ = None
    id = None
    _added = False

    def etl(self):
        self._read_persist()
        while 1:
            self._added = False
            for m in self.__models__:
                print(f'Importing water level model: {m.name}')
                location_table = self.extract(m)
                self.load_locations_things(location_table, m)

            self.offset += self.n
            self._write_persist()
            if self._added:
                if not ask('Continue to next location batch y/[n]'):
                    return

    def delete_thing(self, tid):
        delete_item(f'/Things({tid})')

    def delete_location(self, tid):
        delete_item(f'/Locations({tid})')

    def extract(self, model):
        raise NotImplementedError

    def load_locations_things(self, dbtable, model):
        for record in petl.dicts(dbtable):
            # does this thing have observations via given model
            if self._has_observations(record):
                self._added = True
                location_id = self._post_location(record, model)
                thing_id = self._post_thing(record, model, location_id)
                if self.observation_hook:
                    self._added = self.observation_hook(tids=self._make_tids(thing_id, record),
                                                        models=(model,))

    def _read_persist(self):
        if os.path.isfile(self._persistence_path):
            with open(self._persistence_path, 'r') as rfile:
                yd = yaml.load(rfile)
                self.offset = yd.get('offset', 0)

    def _write_persist(self):
        with open(self._persistence_path, 'w') as wfile:
            yaml.dump({'offset': self.offset}, wfile)

    @property
    def _persistence_path(self):
        return f'{self.id}_persistence.yaml'

    def _has_observations(self, record):
        raise NotImplementedError

    def _post_thing(self, record, model, location_id):
        thing_id = self._get_existing_thing(location_id)
        if thing_id is None:
            thing = self._make_thing(record, location_id)
            thing_id = post_item('Things', thing)
        else:
            print(f'thing {thing_id} already exists')
        return thing_id

    def _post_location(self, record, model):
        location = self._make_location(record)
        location_id = get_item_by_name('Locations', location['name'])
        if location_id is None:
            location_id = post_item('Locations', location)
        else:
            print(f'location {location_id} already exists')
        return location_id

    def _get_existing_thing(self, location_id):
        items = get_items(f'Locations({location_id})?$expand=Things')
        for i in items:
            try:
                for ti in i['Things']:
                    if ti['name'] == self.__thing_name__:
                        return ti['@iot.id']
            except KeyError:
                continue

    def _make_tids(self, tid, record):
        raise NotImplementedError

    def _make_location(self, record):
        raise NotImplementedError

    def _make_thing(self, record, location_id):
        raise NotImplementedError

# ============= EOF =============================================

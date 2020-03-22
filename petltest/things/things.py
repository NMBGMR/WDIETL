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

from petltest import ask, post_item, get_items, get_item_by_name


class BaseThings(object):
    offset = 0
    n = 1
    observation_hook = None
    __thing_name__ = None

    def etl(self):
        while 1:
            for m in self._models:
                print(f'Importing water level model: {m.name}')
                location_table = self.extract_location(m)
                self.load_location_things(location_table, m)

            self.offset += self.n

            if not ask('Continue to next location batch y/[n]'):
                return

    def extract(self, model):
        raise NotImplementedError

    def load_locations_things(self, dbtable, model):
        for record in petl.dicts(dbtable):
            location_id = self._post_location(record, model)
            thing_id = self._post_thing(record, model, location_id)
            if self.observation_hook:
                self.observation_hook(tids=self._make_tids(record, thing_id),
                                      models=(model,))

    def _post_thing(self, record, model, location_id):
        thing_id = self._get_existing_thing(location_id)
        if thing_id is None:
            thing = self._make_thing(record, location_id)
            thing_id = post_item('Things', thing)
        else:
            print(f'thing {thing_id} already exists')
        return thing_id

    def _post_location(self, record, model, location_id):
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

    def _make_tids(self, tid):
        raise NotImplementedError

    def _make_location(self, record):
        raise NotImplementedError

    def _make_thing(self, record, location_id):
        raise NotImplementedError


# ============= EOF =============================================
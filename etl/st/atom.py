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
import requests

from etl.ckan_importer import CKANImporter
from etl.st.base import STBase


class STAtom(STBase):
    __observation_klass__ = None
    __thing_name__ = None
    __models__ = None

    def __init__(self, config):
        self._config = config

        self.ckan_importer = CKANImporter(config)
        self.observation = self.__observation_klass__(config)
        self.observation.ckan_importer = self.ckan_importer

    def extract(self, model, record_id):
        raise NotImplementedError

    def add_package(self, record):
        pkg = self._make_package(record)
        self.ckan_importer.add_package(pkg)

    def _make_package(self, record):
        raise NotImplementedError

    def _has_observations(self, record):
        raise NotImplementedError

    def _post_thing(self, record, model, location_id):
        thing_id = self._get_existing_thing(location_id)
        if thing_id is None:
            thing = self._make_thing(record, location_id)
            thing_id = self._post_item('Things', thing)
        else:
            print(f'thing {thing_id} already exists')
        return thing_id

    def _post_location(self, record, model):
        location = self._make_location(record)
        location_id = self._get_item_by_name('Locations', location['name'])
        if location_id is None:
            location_id = self._post_item('Locations', location)
        else:
            print(f'location {location_id} already exists')
        return location_id

    def _get_existing_thing(self, location_id):
        items = self._get_items(f'Locations({location_id})?$expand=Things')
        for i in items:
            try:
                for ti in i['Things']:
                    if ti['name'] == self.__thing_name__:
                        return ti['@iot.id']
            except KeyError:
                continue

    def _get_items(self, starturl, callback=None):
        items = []

        def _get(u):
            resp = requests.get(u)
            j = resp.json()

            if callback:
                callback(items, j)
            else:
                try:
                    items.extend(j['value'])
                except KeyError:
                    items.append(j)

            try:
                next = j['@iot.nextLink']
            except KeyError:
                return

            _get(next)

        url = self._config.get('gost_url')
        _get(f'{url}/{starturl}')
        return items

    def _make_tids(self, tid, record):
        raise NotImplementedError

    def _make_location(self, record):
        raise NotImplementedError

    def _make_thing(self, record, location_id):
        raise NotImplementedError

# ============= EOF =============================================

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

from etl.st import ConfigBase


class STClient(ConfigBase):

    def update_location(self, location_id, new):
        url = self._config.get('gost_url')
        url = f'{url}/Locations({location_id})'
        resp = requests.patch(url, json=new)
        print('asdf', resp, resp.status_code)

    def get_locations(self, expand=None):
        start = self._make_url('Locations', expand)
        return self._generator(start)

    def get_datastreams(self, expand=None):
        start = self._make_url('Datastreams', expand)
        return self._generator(start)

    def _make_url(self, start, expand):
        if expand:
            start = f'{start}?$expand={expand}'
        return start

    def _generator(self, start):
        def _get(u):
            resp = requests.get(u)
            j = resp.json()
            for i in j['value']:
                yield i

            try:
                next = j['@iot.nextLink']
            except KeyError:
                return

            yield from _get(next)

        url = self._config.get('gost_url')
        yield from _get(f'{url}/{start}')

# ============= EOF =============================================

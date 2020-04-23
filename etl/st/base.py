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


class STBase(ConfigBase):
    def _post_unique_item(self, tag, payload):
        op_id = self._get_item_by_name(tag, payload['name'])
        if op_id is None:
            # add the op
            op_id = self._post_item(tag, payload)
        return op_id

    def _post_item(self, tag, payload):
        url = self._config.get('gost_url')
        resp = requests.post(f'{url}/{tag}', json=payload)
        if resp.status_code == 201:
            return resp.json()['@iot.id']
        else:
            print('failed to post')
            print('============================')
            print(resp.status_code, resp.json())
            print('============================')

    def _get_item_by_name(self, uri, name):
        url = self._config.get('gost_url')
        resp = requests.get(f"{url}/{uri}?$filter=name eq '{name}'")
        try:
            return resp.json()['value'][0]['@iot.id']
        except (KeyError, IndexError, TypeError):
            pass
# ============= EOF =============================================

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
import pytz
import requests

from petltest import get_item_by_name, GOST_URL

MT_TIMEZONE = pytz.timezone('America/Denver')


def get_datastream(thing_id, name):
    uri = f'Things({thing_id})/Datastreams'
    dsid = get_item_by_name(uri, name)
    if dsid is not None:
        print(f'Datastream already exists skipping thing={thing_id}, ds={dsid}, name={name}')

    return dsid


def get_nobservations(datastream_id):
    uri = f'{GOST_URL}/Datastreams({datastream_id})/Observations?$top=1'
    resp = requests.get(uri)
    if resp.status_code == 200:
        try:
            n = int(resp.json()['@iot.count'])
        except KeyError:
            n = 0

        return n
# ============= EOF =============================================

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

from petltest import make_id, post_item, GOST_URL


def add_datastream(thing_id, observed_property_id, sensor_id, ds_payload):
    ds_payload['Thing'] = make_id(thing_id)
    ds_payload['ObservedProperty'] = make_id(observed_property_id)
    ds_payload['Sensor'] = make_id(sensor_id)

    return post_item('Datastreams', ds_payload)


def delete_datastreams(ds):
    for di in ds:
        resp = requests.delete(f'{GOST_URL}/Datastreams({di})')
        print(f'deleting {di}, resp={resp.status_code}')

# ============= EOF =============================================

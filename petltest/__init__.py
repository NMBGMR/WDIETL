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
from os import environ
import pymssql
import requests

db_configs = {'nm_aquifer': {'database': 'NM_Aquifer',
                             'user': environ.get('NM_AQUIFER_DB_USER'),
                             'password': environ.get('NM_AQUIFER_DB_PWD'),
                             'server': environ.get('NM_AQUIFER_DB_HOST')}}


GOST_URL_ROOT = environ.get('GOST_URL_ROOT', 'localhost:8080')
GOST_URL = f'http:/{(GOST_URL_ROOT)}/v1.0'


def get_nm_aquifier_connection():
    config = db_configs['nm_aquifer']
    connection = pymssql.connect(**config)
    return connection


def post_item(uri,  payload):

    resp = requests.post(f'{GOST_URL}/{uri}', json=payload)
    if resp.status_code == 201:
        return resp.json()['@iot.id']
    else:
        print('failed to post')
        print('============================')
        print(resp.status_code, resp.json())
        print('============================')


def get_item_by_name(uri, name):
    resp = requests.get(f"{GOST_URL}/{uri}?$filter=name eq '{name}'")
    try:
        return resp.json()['value'][0]['@iot.id']
    except (KeyError, IndexError):
        pass


def make_id(i):
    return {'@iot.id': i}

# ============= EOF =============================================

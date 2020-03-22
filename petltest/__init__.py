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
                             'server': environ.get('NM_AQUIFER_DB_HOST')},
              'nm_water_quality': {'database': 'NM_Water_Quality',
                                   'user': environ.get('NM_AQUIFER_DB_USER'),
                                   'password': environ.get('NM_AQUIFER_DB_PWD'),
                                   'server': environ.get('NM_AQUIFER_DB_HOST')}}
DEBUG = True

if DEBUG:
    GOST_URL_ROOT = 'localhost:8080'
else:
    GOST_URL_ROOT = environ.get('GOST_URL_ROOT', 'localhost:8080')

GOST_URL = f'http://{(GOST_URL_ROOT)}/v1.0'


def nm_quality_connection():
    config = db_configs['nm_water_quality']
    connection = pymssql.connect(**config)
    return connection


def nm_aquifier_connection():
    config = db_configs['nm_aquifer']
    connection = pymssql.connect(**config)
    return connection


def delete_item(uri):
    resp = requests.delete(f'{GOST_URL}/{uri}')
    print(f'delete thing {resp.status_code}')


def post_item(uri, payload):
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
    except (KeyError, IndexError, TypeError):
        pass


def make_id(i):
    return {'@iot.id': i}


def get_items(starturl, callback=None):
    items = []

    def _get(url):
        resp = requests.get(url)
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

    _get(f'{GOST_URL}/{starturl}')
    return items


def thing_generator(name):
    def get(url):
        resp = requests.get(url)
        j = resp.json()
        try:
            next = j['@iot.nextLink']
        except KeyError:
            return

        for v in j['value']:
            yield v

        # if callback:
        #     callback(items, j)
        # else:
        #     items.extend([v['@iot.id'] for v in j['value']])

        get(next)

    yield from get(f'{GOST_URL}/Things?$filter=name eq \'{name}\'')


YES = ('y',)


def ask(msg):
    return input(msg) in YES
# ============= EOF =============================================

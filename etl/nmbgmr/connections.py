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
import pymssql

from os import environ

db_configs = {'nm_aquifer': {'database': 'NM_Aquifer',
                             'user': environ.get('DB_USER'),
                             'password': environ.get('DB_PWD'),
                             'server': environ.get('DB_HOST')},
              'nm_water_quality': {'database': 'NM_Water_Quality',
                                   'user': environ.get('DB_USER'),
                                   'password': environ.get('DB_PWD'),
                                   'server': environ.get('DB_HOST')}}

def nm_quality_connection():
    config = db_configs['nm_water_quality']
    connection = pymssql.connect(**config)
    return connection


def nm_aquifier_connection():
    config = db_configs['nm_aquifer']
    connection = pymssql.connect(**config)
    return connection
# ============= EOF =============================================

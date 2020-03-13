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
from bonobo_sqlalchemy.util import create_engine
from os import environ

db_configs = {'nm_aquifer': {'name': 'NM_Aquifer',
                             'dialect': 'mssql',
                             'user': environ.get('NM_AQUIFER_DB_USER'),
                             'pwd': environ.get('NM_AQUIFER_DB_PWD'),
                             'host': environ.get('NM_AQUIFER_DB_HOST')}}


def make_engine(name):

    config = db_configs[name]
    connection = f"{config['dialect']}+pymssql://" \
                 f"{config['user']}:{config['pwd']}@{config['host']}/{config['name']}"

    engine = create_engine(connection)
    return engine


def get_services():
    return {'nm_aquifer': make_engine('nm_aquifer')}
# ============= EOF =============================================

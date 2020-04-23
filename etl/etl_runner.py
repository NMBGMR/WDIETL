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
import os
import pprint

import petl
import yaml


# from etl.celery import app
# @app.task
from etl.st.client import STClient


def statom(config, record_id):
    instance = make_instance(config)
    print(f'======= starting etl {instance}, {record_id}')
    instance.etl(record_id)
    print('======= etl complete')


def make_instance(config):
    klass = config['instance']
    if klass == 'WaterLevelPressureAtom':
        from etl.nmbgmr.atoms.wl import WaterLevelPressureAtom
        klass = WaterLevelPressureAtom
    elif klass == 'CABQReport':
        from etl.cabq.cabqreport import CABQReport
        klass = CABQReport

    return klass(config)


class ETLRunner:
    def welcome(self):
        print('=========== ETL Runner ==================================')
        print('Welcome to ETL Runner version 0.0 by Jake Ross 2020')
        print('=========================================================\n\n')

    # ============= commands =============
    def report(self, root):
        config = self._get_config(root)
        print('======== Config =========')
        print('--------- config.yaml')
        print(f'root: {root}')
        pprint.pprint(config)
        print('--------- Environment')
        print(f'DB_HOST: {os.environ.get("DB_HOST")}')
        print(f'DB_USER: {os.environ.get("DB_USER")}')
        print('=========================')

    def batch(self, root):
        self.report(root)
        config = self._get_config(root)

        # just hard coding some batch editing now
        # get all datastreams for WaterLevelsPressure
        client = STClient(config)
        new = {'description': 'WELL'}
        for di in client.get_locations():
            client.update_location(di['@iot.id'], new)

    def run(self, root):
        self.report(root)
        # open the config file
        # get the list of record identifiers to import

        # for each identifier add to worker queue
        config = self._get_config(root)

        i = 0
        for r in self._get_record_identifiers(config):
            if not r.get('imported', 0):
                # statom.delay(config, r['PointID'])
                statom(config, r['PointID'])
                i += 1
                if i > 0:
                    break

    # private
    def _get_record_identifiers(self, cfg):
        p = cfg.get('record_identifiers')
        if not (p and os.path.isfile(p)):
            p = os.path.join(cfg['root'], 'record_ids.json')

        table = petl.fromjson(p)
        return petl.dicts(table)

    def _get_config(self, root):
        with open(os.path.join(root, 'config.yaml'), 'r') as rfile:
            config = yaml.load(rfile, Loader=yaml.Loader)
            config['root'] = root
            return config

# ============= EOF =============================================

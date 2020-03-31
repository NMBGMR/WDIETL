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

import petl
import yaml

from etl.celery import app


@app.task
def statom(config, record_id):
    instance = make_instance(config)
    print(f'etling {instance}, {record_id}')
    instance.etl(record_id)


def make_instance(config):
    klass = config['instance']
    if klass == 'WaterLevelPressureAtom':
        from etl.nmbgmr.atoms.wl import WaterLevelPressureAtom
        klass = WaterLevelPressureAtom

    return klass(config)


class ETLRunner:

    def run(self, root):
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

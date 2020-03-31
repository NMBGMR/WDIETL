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
import petl
import requests
from tqdm import tqdm
import pytz

from etl.st import make_id
from etl.st.base import STBase

MT_TIMEZONE = pytz.timezone('America/Denver')


class BaseObservations(STBase):
    ckan_importer = None
    _sensor_id = None
    _observed_properties = None
    __models__ = None
    __thing_name__ = None

    def etl(self, tids=None, models=None):
        if models is None:
            models = self.__models__

        self._sensor_id = self._add_sensor()

        self._observed_properties = {}
        # add the observed properties
        for m in models:
            payload = m.observed_property_payload
            payload['name'] = m.name
            self._observed_properties[m.name] = self._post_unique_item('ObservedProperties',
                                                                       m.observed_property_payload)

        added = False
        for thing in tids:
            thing_id = thing['@iot.id']
            point_id = thing['@nmbgmr.point_id']
            for m in models:
                print(f'Add {m.name}')
                ds_id = self._get_datastream(thing_id, m.datastream_payload['name'])

                skip_nobs = 0
                if ds_id:
                    # check the number of obs for this datastream matches nrows
                    skip_nobs = self._get_nobservations(ds_id)
                    if skip_nobs:
                        print(f'Skipping nobs={skip_nobs}')

                wt = self._extract(point_id, m, skip_nobs)
                nrows = petl.nrows(wt)
                if nrows:
                    added = True
                    print(f'Adding nobs={nrows}')
                    if not ds_id:
                        ds_id = self._add_datastream(thing_id,
                                                     self._observed_properties[m.name],
                                                     self._sensor_id,
                                                     m.datastream_payload)

                        # r = self._make_resource(m)
                        # self.ckan_importer.add_resource(r)

                    # add observations to datastream
                    self._add_observations(ds_id, wt, m)

        return added

    def _add_sensor(self):
        raise NotImplementedError

    def _make_resource(self, model):
        raise NotImplementedError

    def _extract(self, point_id, model, skip):
        raise NotImplementedError

    def _add_datastream(self, thing_id, observed_property_id, sensor_id, ds_payload):
        ds_payload['Thing'] = make_id(thing_id)
        ds_payload['ObservedProperty'] = make_id(observed_property_id)
        ds_payload['Sensor'] = make_id(sensor_id)

        return self._post_item('Datastreams', ds_payload)

    def _get_nobservations(self, datastream_id):
        url = self._config.get('gost_url')
        uri = f'{url}/Datastreams({datastream_id})/Observations?$top=1'
        resp = requests.get(uri)
        if resp.status_code == 200:
            try:
                n = int(resp.json()['@iot.count'])
            except KeyError:
                n = 0

            return n

    def _get_datastream(self, thing_id, name):
        uri = f'Things({thing_id})/Datastreams'
        dsid = self._get_item_by_name(uri, name)
        if dsid is not None:
            print(f'Datastream already exists skipping thing={thing_id}, ds={dsid}, name={name}')
        return dsid

    def _add_observations(self, datastream_id, records, model):
        # for wti in tqdm(petl.dicts(records)):
        for wti in petl.dicts(records):
            t = MT_TIMEZONE.localize(wti[model.timestamp_column])
            v = wti[model.mapped_column]
            if v is not None:
                payload = {'phenomenonTime': t.isoformat(timespec='milliseconds'),
                           'resultTime': t.isoformat(timespec='milliseconds'),
                           'result': v,
                           'Datastream': make_id(datastream_id)
                           }
                self._post_item(f'Observations', payload)
# ============= EOF =============================================
# wt = self._extract(point_id, m)
# nrows = petl.nrows(wt)
# if nrows:
#     print(f'Add {m.name} observations. count={nrows}')
#     ds_id = get_datastream(thing_id, m.datastream_payload['name'])
#
#     skip_obs = 0
#     add_obs = False
#     if ds_id:
#         # check the number of obs for this datastream matches nrows
#         nobs = get_nobservations(ds_id)
#         if nobs != nrows:
#             print(f'Nobs {nobs} not equal to NRows {nrows}')
#             add_obs = True
#             skip_obs = nobs
#
#     else:
#         add_obs = True
#         ds_id = add_datastream(thing_id,
#                                self._observed_properties[m.name],
#                                self._sensor_id,
#                                m.datastream_payload)
#
#     if add_obs:
#         # add observations to datastream
#         self._add_observations(ds_id, wt, m, skip_obs)

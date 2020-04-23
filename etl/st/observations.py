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
from petl import data
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
            # point_id = thing['@nmbgmr.point_id']
            for m in models:
                print(f'Add {m.name}')
                ds_id = self._get_datastream(thing_id, m.datastream_payload['name'])

                skip_nobs = 0
                if ds_id:
                    print(f'Got datastream  thing={thing_id}, ds={ds_id}')
                    # check the number of obs for this datastream matches nrows
                    skip_nobs = self._get_nobservations(ds_id)
                    if skip_nobs:
                        print(f'Skipping nobs={skip_nobs}')

                wt = self._extract(thing, m, skip_nobs)
                if isinstance(wt, list):
                    nrows = len(wt)
                else:
                    nrows = petl.nrows(wt)

                if nrows:
                    added = True
                    print(f'Adding nobs={nrows}')
                    if not ds_id:
                        print('Adding datastream')

                        sensor_id = self._sensor_id
                        if self._sensor_id is None:
                            # extract the sensor from the first record
                            sensor_id = self._add_sensor(wt)

                        ds_id = self._add_datastream(thing_id,
                                                     self._observed_properties[m.name],
                                                     sensor_id,
                                                     m.datastream_payload)

                        # r = self._make_resource(m)
                        # self.ckan_importer.add_resource(r)

                    # add observations to datastream
                    self._add_observations(ds_id, wt, m)
                else:
                    print('no obs to add')

        return added

    def set_records(self, records):
        self._records = records

    def _add_sensor(self, *args, **kw):
        raise NotImplementedError

    def _make_resource(self, model):
        raise NotImplementedError

    def _extract(self, thing, model, skip):
        if self._records:
            r = self._records[skip:]
            return r
        else:
            raise NotImplementedError

    def _add_datastream(self, thing_id, observed_property_id, sensor_id, ds_payload):
        ds_payload['Thing'] = make_id(thing_id)
        ds_payload['ObservedProperty'] = make_id(observed_property_id)
        ds_payload['Sensor'] = make_id(sensor_id)

        return self._post_item('Datastreams', ds_payload)

    def _get_nobservations(self, datastream_id):
        url = self._config.get('gost_url')
        uri = f'{url}/Datastreams({datastream_id})/Observations?$count=true&$top=1'
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

        return dsid

    def _timestamp_extract(self, t):
        return t

    def _add_observations(self, datastream_id, records, model):
        if not isinstance(records, list):
            records = petl.dicts(records)

        for wti in tqdm(records):

            t = self._timestamp_extract(wti[model.timestamp_column])

            t = MT_TIMEZONE.localize(t)
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

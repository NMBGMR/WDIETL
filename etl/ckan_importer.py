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

import requests
from ckanapi import RemoteCKAN
from ckanapi.errors import ValidationError


class CKANImporter(object):
    _package_id = None

    def __init__(self, config):
        self._config = config

    def ckan_ctx(self):
        url = self._config.get('ckan_url')
        apikey = self._config.get('ckan_apikey')
        return RemoteCKAN(url, apikey=apikey)

    def add_package(self, package):
        with self.ckan_ctx() as ckan:
            try:
                ckan.action.package_create(**package)
            except (ValidationError, requests.ConnectionError):
                pass

        self._package_id = package['name']

    def add_resource(self, resource):
        with self.ckan_ctx() as ckan:
            if 'package_id' not in resource:
                resource['package_id'] = self._package_id
            try:
                ckan.action.resource_create(**resource)
            except (ValidationError, requests.ConnectionError):
                pass

    # @ckanwrapper
    # def add_resources(self, ckan, package_id=None):
    #     if package_id is None:
    #         package_id = self._package_id
    #
    #     # # loop thru existing datastreams adding to ckan
    #     for ds in datastream_generator():
    #         print(ds)
    #         resource = self._make_resource_from_datastream(package_id, ds)
    #         ckan.action.resource_create(**resource)
    #         break
    #
    # def _make_resource_from_datastream(self, package_id, ds):
    #     thing = ds['Thing']
    #     props = thing['properties']
    #     point_id = props['@nmbgmr.point_id']
    #
    #     name = ds['name']
    #     r = {'package_id': package_id,
    #          'name': f'{point_id}-{name}',
    #          'url': ds['@iot.selfLink'],
    #          'resource_type': 'api',
    #          'description': ds['description'],
    #          }
    #     return r

# ============= EOF =============================================
# resp = ckan.action.current_package_list_with_resources()
# for ri in resp:
#     for rr in ri['resources']:
#         # print(rr)
#         ckan.action.resource_delete(id=rr['id'])

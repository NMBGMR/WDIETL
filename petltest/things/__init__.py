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
import json


def dump_thing_mapping(obj, path):
    if obj:
        with open(f'{path}.json', 'w') as wfile:
            json.dump(obj, wfile, indent=2)


def make_thing(ld, column_mapping):

    return {'name': ld[column_mapping['name']],
            'description': 'Water Well',
            'properties': {},
            'Locations': [{'name': ld[column_mapping['location_name']] or 'No Name',
                           'description': 'No Description',
                           'encodingType': 'application/vnd.geo+json',
                           'location': {'type': 'Point',
                                        'coordinates': [ld['LongitudeDD'],
                                                        ld['LatitudeDD']]}
                           }]
            }

# ============= EOF =============================================

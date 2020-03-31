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

# water chem
from wdietl.ckan_importer import CKANImporter
from wdietl.datastreams import datastream_generator
from wdietl.observations.wl_observations import WaterLevelPressureObservations
from wdietl.observations.wq_observations import WaterChemistryObservations
from wdietl.things.wl_things import WaterLevelPressureThings
from wdietl.things.wq_things import WaterChemThings


# water levels
# from petltest.things.wl_things import etl_things as etl_wl_things
# from petltest.observations.wl_observations import etl_wl_observations


def main():
    # ====== CKAN import ===
    c = CKANImporter()
    c.run_test()

    # ====== water chem =====
    # wc = WaterChemThings()
    # wc.n = 3
    # wc.observation_hook = WaterChemistryObservations().etl
    # wc.etl()
    # =======================

    # ==== water levels =====
    # wl = WaterLevelPressureThings()
    # wl.n = 3
    # wl.observation_hook = WaterLevelPressureObservations().etl
    # wl.etl()
    # =======================

    # wl.delete_location(3)
    # wl.delete_thing(2)
    # wl.delete_thing(3)
    # wl.find_test_waterlevels_pointid()


if __name__ == '__main__':
    main()
# ============= EOF =============================================

# def delete_all_things():
#     for i in range(1000):
#         resp = requests.delete(f'{GOST_URL}/Things({i})')
#         if not resp.status_code == 200:
#             print('delete thing', i, resp.json())
#
#
# def delete_all_locations():
#     for i in range(1000):
#         resp = requests.delete(f'{GOST_URL}/Locations({i})')
#         if not resp.status_code == 200:
#             print('delete location', i, resp.json())
#
#
# def cleanup():
#     delete_all_things()
#     delete_all_locations()

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

# from petltest.datastreams import delete_datastreams
# from petltest.observations.wl_observations import etl_wl_observations
# from petltest.things import get_things
# from petltest.things.wl_things import etl_things as etl_wl_things

from petltest.observations.wq_observations import etl_wq_observations
from petltest.things.wq_things import etl_things as etl_wq_things


def main():

    etl_wq_things(observation_hook=etl_wq_observations)

    # etl_wq_observations()

    # etl_wl_things()
    # delete_datastreams((1,2,3,4,5,6,7))
    # etl_wl_observations()
    # get_things()


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

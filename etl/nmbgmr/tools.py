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

import petl

from nmbgmr.connections import nm_aquifier_connection


def make_runlist():
    sql = '''select DISTINCT Location.PointID from dbo.WaterLevelsContinuous_Pressure
join dbo.Location on Location.PointID = dbo.WaterLevelsContinuous_Pressure.PointID
where dbo.Location.LatitudeDD is not null and dbo.Location.PublicRelease=1
group by Location.PointID
order by Location.PointID'''
    table = petl.fromdb(nm_aquifier_connection(), sql)

    obj = petl.tojson(table, 'record_ids.json', indent=2)
    print(obj)


if __name__ == '__main__':
    make_runlist()
# ============= EOF =============================================

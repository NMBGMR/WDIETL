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
import bonobo
import bonobo_sqlalchemy

import services


def get_graph():
    graph = bonobo.Graph()
    graph.add_chain(
        bonobo_sqlalchemy.Select('SELECT * FROM dbo.Location where PublicRelease=1',
                                 engine='nm_aquifer'),
        bonobo.PrettyPrinter(),
    )
    return graph


def main():
    bonobo.run(get_graph(), services=services.get_services())


if __name__ == '__main__':
    main()
# ============= EOF =============================================

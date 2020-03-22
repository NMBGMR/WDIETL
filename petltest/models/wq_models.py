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
from petltest.models import BaseModel, PPM


class WQModel(BaseModel):
    timestamp_column = 'CollectionTime'


ARSENIC = WQModel('Arsenic', 'Arsenic',
                  {'description': 'Arsenic',
                   'definition': 'No Definition'},
                  {'name': 'Arsenic DS',
                   'description': 'Datastream for Arsenic',
                   'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                   'unitOfMeasurement': PPM
                   }
                  )

CA = WQModel('Calcium', 'Ca',
             {'description': 'Calcium',
              'definition': 'No Definition'},
             {'name': 'Calcium DS',
              'description': 'Datastream for Calcium',
              'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
              'unitOfMeasurement': PPM
              }
             )

CL = WQModel('Chlorine', 'Cl',
             {'description': 'Chlorine',
              'definition': 'No Definition'},
             {'name': 'Chlorine DS',
              'description': 'Datastream for Chlorine',
              'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
              'unitOfMeasurement': PPM
              }
             )

F = WQModel('Fluoride', 'F',
            {'description': 'Fluoride',
             'definition': 'No Definition'},
            {'name': 'Fluoride DS',
             'description': 'Datastream for Fluoride',
             'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
             'unitOfMeasurement': PPM
             }
            )

MG = WQModel('Magnesium', 'Mg',
             {'description': 'Magnesium',
              'definition': 'No Definition'},
             {'name': 'Magnesium DS',
              'description': 'Datastream for Magnesium',
              'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
              'unitOfMeasurement': PPM
              }
             )

NA = WQModel('Sodium', 'Na',
             {'description': 'Sodium',
              'definition': 'No Definition'},
             {'name': 'Sodium DS',
              'description': 'Datastream for Sodium',
              'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
              'unitOfMeasurement': PPM
              }
             )

SO4 = WQModel('Sulfate', 'SO4',
              {'description': 'Sulfate',
               'definition': 'No Definition'},
              {'name': 'Sulfate DS',
               'description': 'Datastream for Sulfate',
               'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
               'unitOfMeasurement': PPM
               }
              )
# ============= EOF =============================================

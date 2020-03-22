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
from petltest.models import BaseModel, FOOT, DEGC


class WaterLevelModel(BaseModel):
    timestamp_column = 'DateMeasured'


WATER_HEAD = WaterLevelModel('WaterHead', 'WaterHead',
                             {'description': 'Water Head above sensor',
                              'definition': 'No Definition'},
                             {'name': 'Water Head DS',
                              'description': 'Datastream for water head',
                              'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                              'unitOfMeasurement': FOOT
                              }
                             )

WATER_HEAD_ADJUSTED = WaterLevelModel("WaterHeadAdjusted", 'WaterHeadAdjusted',
                                      {'description': 'Adjusted Water Head above sensor',
                                       'definition': 'No Definition'},
                                      {'name': 'Water Head Adjusted DS',
                                       'description': 'Datastream for water head adjusted',
                                       'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                                       'unitOfMeasurement': FOOT
                                       }
                                      )

WATER_TEMPERATURE = WaterLevelModel('WaterTemperature', 'TemperatureWater',
                                    {'description': 'Temperature of the water in the well',
                                     'definition': 'No Definition'},
                                    {'name': 'Water Temperature DS',
                                     'description': 'Datastream for water temperature',
                                     'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                                     'unitOfMeasurement': DEGC
                                     }
                                    )

WATER_CONDUCTIVITY = WaterLevelModel('WaterConductivity', 'CONDDL (mS/cm)',
                                     {'description': 'Electrical conductivity of the water',
                                      'definition': 'No Definition'},
                                     {'name': 'Water Conductivity DS',
                                      'description': 'Datastream for water electrical conductivity',
                                      'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                                      'unitOfMeasurement': {'name': 'millisiemens per cm',
                                                            'symbol': 'mS/cm',
                                                            'definition': 'https://en.wikipedia.org/wiki/Conductivity_(electrolytic)'}
                                      }

                                     )

DEPTH_TO_WATER = WaterLevelModel('DepthToWater', 'DepthToWaterBGS',
                                 {'description': 'Depth to water below the ground surface',
                                  'definition': 'No Definition'},
                                 {'name': 'Depth to Water below ground surface DS',
                                  'description': 'Datastream for the depth to water below ground surface',
                                  'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                                  'unitOfMeasurement': FOOT
                                  }
                                 )

AIR_TEMPERATURE = WaterLevelModel('AirTemperature', 'TemperatureAir',
                                  {'description': 'Air temperature at the surface',
                                   'definition': 'No Definition'},
                                  {'name': 'Air Temperature DS',
                                   'description': 'Datastream for air temperature',
                                   'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                                   'unitOfMeasurement': DEGC
                                   }
                                  )

# ============= EOF =============================================

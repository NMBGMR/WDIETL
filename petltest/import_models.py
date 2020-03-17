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

class BaseModel():
    def __init__(self, name, mapped_column, ob, ds):
        self.name = name
        self.mapped_column = mapped_column
        self.observed_property_payload = ob
        self.datastream_payload = ds


FOOT = {'name': 'Foot',
        'symbol': 'ft',
        'definition': 'http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Foot'}
DEGC = {'name': 'Degree Celsius',
        'symbol': 'degC',
        'definition': 'http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius'}

PPM = {'name': 'Parts Per Million',
       'symbol': 'PPM',
       'definition': 'http://www.qudt.org/qudt/owl/1.0.0'}

WATER_HEAD = BaseModel('WaterHead', 'WaterHead',
                       {'description': 'Water Head above sensor',
                        'definition': 'No Definition'},
                       {'name': 'Water Head DS',
                        'description': 'Datastream for water head',
                        'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                        'unitOfMeasurement': FOOT
                        }
                       )

WATER_HEAD_ADJUSTED = BaseModel("WaterHeadAdjusted", 'WaterHeadAdjusted',
                                {'description': 'Adjusted Water Head above sensor',
                                 'definition': 'No Definition'},
                                {'name': 'Water Head Adjusted DS',
                                 'description': 'Datastream for water head adjusted',
                                 'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                                 'unitOfMeasurement': FOOT
                                 }
                                )

WATER_TEMPERATURE = BaseModel('WaterTemperature', 'TemperatureWater',
                              {'description': 'Temperature of the water in the well',
                               'definition': 'No Definition'},
                              {'name': 'Water Temperature DS',
                               'description': 'Datastream for water temperature',
                               'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                               'unitOfMeasurement': DEGC
                               }
                              )

WATER_CONDUCTIVITY = BaseModel('WaterConductivity', 'CONDDL',
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

DEPTH_TO_WATER = BaseModel('DepthToWater', 'DepthToWaterBGS',
                           {'description': 'Depth to water below the ground surface',
                            'definition': 'No Definition'},
                           {'name': 'Depth to Water below ground surface DS',
                            'description': 'Datastream for the depth to water below ground surface',
                            'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                            'unitOfMeasurement': FOOT
                            }
                           )

AIR_TEMPERATURE = BaseModel('AirTemperature', 'TemperatureAir',
                            {'description': 'Air temperature at the surface',
                             'definition': 'No Definition'},
                            {'name': 'Air Temperature DS',
                             'description': 'Datastream for air temperature',
                             'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                             'unitOfMeasurement': DEGC
                             }
                            )

ARSENIC = BaseModel('Arsenic', 'Arsenic',
                    {'description': 'Arsenic',
                     'definition': 'No Definition'},
                    {'name': 'Arsenic DS',
                     'description': 'Datastream for Arsenic',
                     'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                     'unitOfMeasurement': PPM
                     }
                    )

CA = BaseModel('Calcium', 'Ca',
               {'description': 'Calcium',
                'definition': 'No Definition'},
               {'name': 'Calcium DS',
                'description': 'Datastream for Calcium',
                'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                'unitOfMeasurement': PPM
                }
               )
# ============= EOF =============================================

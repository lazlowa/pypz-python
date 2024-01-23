# =============================================================================
# Copyright (c) 2024 by Laszlo Anka. All rights reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================
import unittest

from pypz.core.specs.pipeline import Pipeline
from core.test.abstracts_tests.channel_ports_resources import TestPipelineWithReplicatedOperators


class PipelineTest(unittest.TestCase):

    def test_pipeline_creation_from_json_of_existing_pipeline_expect_equality(self):
        json_string = """
        {
          "name": "pipeline",
          "parameters": {},
          "dependsOn": [],
          "spec": {
            "name": "core.test.abstracts_tests.channel_ports_resources:TestPipelineWithReplicatedOperators",
            "location": null,
            "expectedParameters": {},
            "types": [
              "<class 'pypz.core.specs.pipeline.Pipeline'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.operator.Operator'>",
            "nestedInstances": [
              {
                "name": "writer",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 2                  
                },
                "dependsOn": [],
                "spec": {
                  "name": "core.test.abstracts_tests.channel_ports_resources:TestWriterOperator",
                  "location": null,
                  "expectedParameters": {
                    "operatorImageName": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": null
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.operator.Operator'>"
                  ],
                  "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
                  "nestedInstances": [
                    {
                      "name": "output_port",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "portOpenTimeoutMs": 0
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "core.test.abstracts_tests.channel_ports_resources:TestChannelOutputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.OutputPortPlugin'>",
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "logger",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "pypz.plugins.loggers.default:DefaultLoggerPlugin",
                        "location": null,
                        "expectedParameters": {
                          "logLevel": {
                            "type": "str",
                            "required": false,
                            "description": null,
                            "currentValue": "DEBUG"
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.LoggerPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": []
              },
              {
                "name": "reader",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 2
                },
                "dependsOn": [],
                "spec": {
                  "name": "core.test.abstracts_tests.channel_ports_resources:TestReaderOperator",
                  "location": null,
                  "expectedParameters": {
                    "operatorImageName": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": null
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.operator.Operator'>"
                  ],
                  "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
                  "nestedInstances": [
                    {
                      "name": "logger",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "pypz.plugins.loggers.default:DefaultLoggerPlugin",
                        "location": null,
                        "expectedParameters": {
                          "logLevel": {
                            "type": "str",
                            "required": false,
                            "description": null,
                            "currentValue": "DEBUG"
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.LoggerPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "input_port_b",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "sequentialModeEnabled": false,
                        "portOpenTimeoutMs": 0,
                        "syncConnectionsOpen": false
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "core.test.abstracts_tests.channel_ports_resources:TestChannelInputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "sequentialModeEnabled": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          },
                          "syncConnectionsOpen": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.InputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "input_port_a",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "sequentialModeEnabled": false,
                        "portOpenTimeoutMs": 0,
                        "syncConnectionsOpen": false
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "core.test.abstracts_tests.channel_ports_resources:TestChannelInputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "sequentialModeEnabled": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          },
                          "syncConnectionsOpen": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.InputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": [
                  {
                    "inputPortName": "input_port_b",
                    "source": {
                      "instanceName": "writer",
                      "outputPortName": "output_port"
                    }
                  },
                  {
                    "inputPortName": "input_port_a",
                    "source": {
                      "instanceName": "writer",
                      "outputPortName": "output_port"
                    }
                  }
                ]
              }
            ]
          }
        }

        """

        ref_pipeline = TestPipelineWithReplicatedOperators("pipeline")
        pipeline = Pipeline.create_from_string(json_string)

        self.assertEqual(ref_pipeline, pipeline)
        self.assertEqual(1, len(ref_pipeline.reader.input_port_a.get_connected_ports()))
        self.assertIn(ref_pipeline.writer.output_port, ref_pipeline.reader.input_port_a.get_connected_ports())
        self.assertEqual(1, len(ref_pipeline.reader.get_replica(0).input_port_a.get_connected_ports()))
        self.assertIn(ref_pipeline.writer.output_port, ref_pipeline.reader.get_replica(0).input_port_a.get_connected_ports())

        self.assertEqual(2, len(ref_pipeline.writer.output_port.get_connected_ports()))
        self.assertIn(ref_pipeline.reader.input_port_a, ref_pipeline.writer.output_port.get_connected_ports())

        self.assertEqual(2, len(ref_pipeline.writer.get_replica(0).output_port.get_connected_ports()))
        self.assertIn(ref_pipeline.reader.input_port_a, ref_pipeline.writer.get_replica(0).output_port.get_connected_ports())

    def test_pipeline_creation_from_json_with_mocked_instances_expect_existing_connections(self):
        json_string = """
        {
          "name": "pipeline",
          "parameters": {},
          "dependsOn": [],
          "spec": {
            "name": "dummy:TestPipelineWithReplicatedOperators",
            "location": null,
            "expectedParameters": {},
            "types": [
              "<class 'pypz.core.specs.pipeline.Pipeline'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.operator.Operator'>",
            "nestedInstances": [
              {
                "name": "writer",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 2
                },
                "dependsOn": [],
                "spec": {
                  "name": "dummy:TestWriterOperator",
                  "location": null,
                  "expectedParameters": {
                    "operatorImageName": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": null
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.operator.Operator'>"
                  ],
                  "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
                  "nestedInstances": [
                    {
                      "name": "logger",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "pypz.plugins.loggers.default:DefaultLoggerPlugin",
                        "location": null,
                        "expectedParameters": {
                          "logLevel": {
                            "type": "str",
                            "required": false,
                            "description": null,
                            "currentValue": "DEBUG"
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.LoggerPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "output_port",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "portOpenTimeoutMs": 0
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "dummy:TestChannelOutputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.OutputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": []
              },
              {
                "name": "reader",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 2,
                },
                "dependsOn": [],
                "spec": {
                  "name": "dummy:TestReaderOperator",
                  "location": null,
                  "expectedParameters": {
                    "operatorImageName": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": null
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.operator.Operator'>"
                  ],
                  "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
                  "nestedInstances": [
                    {
                      "name": "input_port_b",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "sequentialModeEnabled": false,
                        "portOpenTimeoutMs": 0,
                        "syncConnectionsOpen": false
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "dummy:TestChannelInputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "sequentialModeEnabled": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          },
                          "syncConnectionsOpen": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.InputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "input_port_a",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "sequentialModeEnabled": false,
                        "portOpenTimeoutMs": 0,
                        "syncConnectionsOpen": false
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "dummy:TestChannelInputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "sequentialModeEnabled": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          },
                          "syncConnectionsOpen": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.InputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "logger",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "pypz.plugins.loggers.default:DefaultLoggerPlugin",
                        "location": null,
                        "expectedParameters": {
                          "logLevel": {
                            "type": "str",
                            "required": false,
                            "description": null,
                            "currentValue": "DEBUG"
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.LoggerPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": [
                  {
                    "inputPortName": "input_port_a",
                    "source": {
                      "instanceName": "writer",
                      "outputPortName": "output_port"
                    }
                  },
                  {
                    "inputPortName": "input_port_b",
                    "source": {
                      "instanceName": "writer",
                      "outputPortName": "output_port"
                    }
                  }
                ]
              }
            ]
          }
        }
        """

        pipeline = Pipeline.create_from_string(json_string, mock_nonexistent=True)

        self.assertEqual(1, len(pipeline.reader.input_port_a.get_connected_ports()))
        self.assertIn(pipeline.writer.output_port, pipeline.reader.input_port_a.get_connected_ports())
        self.assertEqual(1, len(pipeline.reader.get_replica(0).input_port_a.get_connected_ports()))
        self.assertIn(pipeline.writer.output_port, pipeline.reader.get_replica(0).input_port_a.get_connected_ports())

        self.assertEqual(2, len(pipeline.writer.output_port.get_connected_ports()))
        self.assertIn(pipeline.reader.input_port_a, pipeline.writer.output_port.get_connected_ports())

        self.assertEqual(2, len(pipeline.writer.get_replica(0).output_port.get_connected_ports()))
        self.assertIn(pipeline.reader.input_port_a, pipeline.writer.get_replica(0).output_port.get_connected_ports())

    def test_pipeline_creation_from_json_with_one_operator_mocked_expect_existing_connections(self):
        json_string = """
        {
          "name": "pipeline",
          "parameters": {},
          "dependsOn": [],
          "spec": {
            "name": "dummy:TestPipelineWithReplicatedOperators",
            "location": null,
            "expectedParameters": {},
            "types": [
              "<class 'pypz.core.specs.pipeline.Pipeline'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.operator.Operator'>",
            "nestedInstances": [
              {
                "name": "writer",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 2              
                },
                "dependsOn": [],
                "spec": {
                  "name": "core.test.abstracts_tests.channel_ports_resources:TestWriterOperator",
                  "location": null,
                  "expectedParameters": {
                    "operatorImageName": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": null
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.operator.Operator'>"
                  ],
                  "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
                  "nestedInstances": [
                    {
                      "name": "logger",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "pypz.plugins.loggers.default:DefaultLoggerPlugin",
                        "location": null,
                        "expectedParameters": {
                          "logLevel": {
                            "type": "str",
                            "required": false,
                            "description": null,
                            "currentValue": "DEBUG"
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.LoggerPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "output_port",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "portOpenTimeoutMs": 0
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "core.test.abstracts_tests.channel_ports_resources:TestChannelOutputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.OutputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": []
              },
              {
                "name": "reader",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 2                  
                },
                "dependsOn": [],
                "spec": {
                  "name": "dummy:TestReaderOperator",
                  "location": null,
                  "expectedParameters": {
                    "operatorImageName": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": null
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.operator.Operator'>"
                  ],
                  "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
                  "nestedInstances": [
                    {
                      "name": "input_port_b",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "sequentialModeEnabled": false,
                        "portOpenTimeoutMs": 0,
                        "syncConnectionsOpen": false
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "core.test.abstracts_tests.channel_ports_resources:TestChannelInputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "sequentialModeEnabled": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          },
                          "syncConnectionsOpen": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.InputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "input_port_a",
                      "parameters": {
                        "channelLocation": null,
                        "channelConfig": {},
                        "sequentialModeEnabled": false,
                        "portOpenTimeoutMs": 0,
                        "syncConnectionsOpen": false
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "core.test.abstracts_tests.channel_ports_resources:TestChannelInputPort",
                        "location": null,
                        "expectedParameters": {
                          "channelLocation": {
                            "type": "str",
                            "required": true,
                            "description": null,
                            "currentValue": null
                          },
                          "channelConfig": {
                            "type": "dict",
                            "required": false,
                            "description": null,
                            "currentValue": {}
                          },
                          "sequentialModeEnabled": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          },
                          "portOpenTimeoutMs": {
                            "type": "int",
                            "required": false,
                            "description": null,
                            "currentValue": 0
                          },
                          "syncConnectionsOpen": {
                            "type": "bool",
                            "required": false,
                            "description": null,
                            "currentValue": false
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.ExtendedPlugin'>",
                          "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>",
                          "<class 'pypz.core.specs.plugin.InputPortPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "logger",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "pypz.plugins.loggers.default:DefaultLoggerPlugin",
                        "location": null,
                        "expectedParameters": {
                          "logLevel": {
                            "type": "str",
                            "required": false,
                            "description": null,
                            "currentValue": "DEBUG"
                          }
                        },
                        "types": [
                          "<class 'pypz.core.specs.plugin.LoggerPlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": [
                  {
                    "inputPortName": "input_port_a",
                    "source": {
                      "instanceName": "writer",
                      "outputPortName": "output_port"
                    }
                  },
                  {
                    "inputPortName": "input_port_b",
                    "source": {
                      "instanceName": "writer",
                      "outputPortName": "output_port"
                    }
                  }
                ]
              }
            ]
          }
        }
        """

        pipeline = Pipeline.create_from_string(json_string, mock_nonexistent=True)

        self.assertEqual(1, len(pipeline.reader.input_port_a.get_connected_ports()))
        self.assertIn(pipeline.writer.output_port, pipeline.reader.input_port_a.get_connected_ports())
        self.assertEqual(1, len(pipeline.reader.get_replica(0).input_port_a.get_connected_ports()))
        self.assertIn(pipeline.writer.output_port, pipeline.reader.get_replica(0).input_port_a.get_connected_ports())

        self.assertEqual(2, len(pipeline.writer.output_port.get_connected_ports()))
        self.assertIn(pipeline.reader.input_port_a, pipeline.writer.output_port.get_connected_ports())

        self.assertEqual(2, len(pipeline.writer.get_replica(0).output_port.get_connected_ports()))
        self.assertIn(pipeline.reader.input_port_a, pipeline.writer.get_replica(0).output_port.get_connected_ports())
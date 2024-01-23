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
from typing import Optional

from pypz.core.commons.parameters import RequiredParameter
from pypz.core.specs.misc import BlankOperator, BlankServicePlugin
from pypz.core.specs.pipeline import Pipeline
from pypz.executors.commons import ExecutionMode
from pypz.executors.pipeline.executor import PipelineExecutor
from pypz.plugins.loggers.default import DefaultLoggerPlugin


class TestServicePlugin(BlankServicePlugin):

    def _on_service_start(self) -> bool:
        self.get_logger().info("OnServiceStart")
        return True

    def _on_service_stop(self) -> bool:
        self.get_logger().info("OnServiceStart")
        return True


class TestOperator(BlankOperator):

    req = RequiredParameter(str, description="Test required parameter")

    def __init__(self, name: str = None, replication_factor: int = None, *args, **kwargs):
        super().__init__(name, replication_factor, *args, **kwargs)

        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")

        self.service = TestServicePlugin()
        self.service2 = TestServicePlugin()

        self.req = None

    def _on_running(self) -> Optional[bool]:
        self.get_logger().info("Running")
        return True


class TestPipeline(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.operator_a: BlankOperator = TestOperator()
        self.operator_a.set_parameter("replicationFactor", 1)

        self.operator_b: BlankOperator = TestOperator()
        self.operator_c: BlankOperator = TestOperator()
        self.operator_d: BlankOperator = TestOperator()


class PipelineExecutorTest(unittest.TestCase):

    def test_pipeline_execution_expect_success(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">req", "test")

        try:
            executor = PipelineExecutor(pipeline)
            executor.start(ExecutionMode.Standard)
            executor.shutdown()
        except:  # noqa: E722
            self.fail()

    def test_pipeline_execution_with_mocked_operator_expect_early_termination(self):
        pipeline_json = """
        {
          "name": "pipeline",
          "parameters": {},
          "dependsOn": [],
          "spec": {
            "name": "dummy:TestPipeline",
            "location": null,
            "expectedParameters": {},
            "types": [
              "<class 'pypz.core.specs.pipeline.Pipeline'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.operator.Operator'>",
            "nestedInstances": [
              {
                "name": "operator_b",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 0,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "executor_test:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
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
                "name": "operator_d",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 0,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "executor_test:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
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
                "name": "operator_c",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 0,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "dummy:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
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
                "name": "operator_a",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 1,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "executor_test:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": []
              }
            ]
          }
        }
        """

        pipeline = Pipeline.create_from_string(pipeline_json, mock_nonexistent=True)

        with self.assertRaises(PermissionError):
            PipelineExecutor(pipeline)

    def test_pipeline_execution_with_mocked_plugin_in_non_mocked_operator_expect_early_termination(self):
        pipeline_json = """
        {
          "name": "pipeline",
          "parameters": {},
          "dependsOn": [],
          "spec": {
            "name": "dummy:TestPipeline",
            "location": null,
            "expectedParameters": {},
            "types": [
              "<class 'pypz.core.specs.pipeline.Pipeline'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.operator.Operator'>",
            "nestedInstances": [
              {
                "name": "operator_b",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 0,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "executor_test:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
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
                "name": "operator_d",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 0,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "executor_test:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
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
                "name": "operator_c",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 0,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "dummy:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
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
                "name": "operator_a",
                "parameters": {
                  "operatorImageName": null,
                  "replicationFactor": 1,
                  "req": "test"
                },
                "dependsOn": [],
                "spec": {
                  "name": "executor_test:TestOperator",
                  "location": null,
                  "expectedParameters": {
                    "req": {
                      "type": "str",
                      "required": true,
                      "description": "Test required parameter",
                      "currentValue": "test"
                    },
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
                      "name": "mocked",
                      "parameters": {
                        "logLevel": "DEBUG"
                      },
                      "dependsOn": [],
                      "spec": {
                        "name": "dummy:DefaultLoggerPlugin",
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
                      "name": "service",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    },
                    {
                      "name": "service2",
                      "parameters": {},
                      "dependsOn": [],
                      "spec": {
                        "name": "executor_test:TestServicePlugin",
                        "location": null,
                        "expectedParameters": {},
                        "types": [
                          "<class 'pypz.core.specs.plugin.ServicePlugin'>"
                        ],
                        "nestedInstanceType": null,
                        "nestedInstances": null
                      }
                    }
                  ]
                },
                "connections": []
              }
            ]
          }
        }
        """

        pipeline = Pipeline.create_from_string(pipeline_json, mock_nonexistent=True)

        with self.assertRaises(PermissionError):
            PipelineExecutor(pipeline)

    def test_pipeline_execution_with_missing_required_parameter_expect_error(self):
        pipeline = TestPipeline("pipeline")

        with self.assertRaises(LookupError):
            PipelineExecutor(pipeline)

from plugins.amqp_io.test.resources import TestPipeline
from pypz.executors.operator.executor import OperatorExecutor

pipeline = TestPipeline("amqp_pipeline")
pipeline.set_parameter(">>channelLocation", "localhost:5672")

executor = OperatorExecutor(pipeline.writer)
executor.execute()

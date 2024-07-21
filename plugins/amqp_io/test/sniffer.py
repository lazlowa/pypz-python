from plugins.amqp_io.test.resources import TestPipeline
from pypz.sniffer.viewer import PipelineSnifferViewer

pipeline = TestPipeline("amqp_pipeline")
pipeline.set_parameter(">>channelLocation", "localhost:5672")

sniffer = PipelineSnifferViewer(pipeline)
sniffer.mainloop()

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
import sys

from .viewer import PipelineSnifferViewer
from pypz.core.specs.pipeline import Pipeline

if __name__ == "__main__":
    if 1 == len(sys.argv):
        raise AttributeError("You must provide a valid pipeline instance json configuration.")

    pipeline = Pipeline.create_from_string(sys.argv[0])
    pipeline_sniffer_viewer = PipelineSnifferViewer(pipeline)
    pipeline_sniffer_viewer.mainloop()

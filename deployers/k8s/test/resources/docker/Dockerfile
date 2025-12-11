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
FROM python:3.11-alpine3.18

ARG BUILD_VERSION

ENV CONTAINER_ROOT=/operator
ENV CONTAINER_CONFIG=$CONTAINER_ROOT/config
ENV PYTHON_PROJECT=$CONTAINER_ROOT/project

ENV PYTHONPATH=/operator/project
ENV PYPZ_VERSION=$BUILD_VERSION

RUN mkdir -p $CONTAINER_CONFIG && \
    chmod 777 -R $CONTAINER_ROOT

WORKDIR $CONTAINER_ROOT

RUN /usr/local/bin/python -m pip install --upgrade pip

COPY .. .

RUN pip install -e \
    core \
    deployers/k8s \
    plugins/kafka_io \
    plugins/rmq_io

ENTRYPOINT ["/bin/sh"]
CMD ["deployers/k8s/test/resources/docker/pypz-startup.sh"]

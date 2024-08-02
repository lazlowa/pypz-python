#!/usr/bin/env bash

set -e

SCRIPT_PATH=$(dirname "$(realpath "$0")")

docker run -d --rm \
  --name rabbitmq \
  --hostname localhost \
  -p 15672:15672 \
  -p 5672:5672 \
  -p 5552:5552 \
  -v "$SCRIPT_PATH/enabled_plugins":/etc/rabbitmq/enabled_plugins
  rabbitmq:3-management

# Description

This package contains the RabbitMQ implementation of the Input-/OutputPortPlugin 
interface of *pypz*. It enables the operators to send and receive data records
through queues allowing load sharing.

Check the [documentation](https://lazlowa.github.io/pypz-python/plugins/rmq_io.html) for
more details.

# Install

The python artifact is hosted on https://pypi.org/, so you can install
it via pip:

```shell
pip install pypz-rmq-io
```

If you want to work on it locally, then you should install in editable mode:

```shell
pip install -e ./plugins/rmq_io
```

# Test

You can run tests against an existing RabbitMQ cluster, however, it is
more convenient and safe to use a local test cluster.


1. Run the [official image](https://hub.docker.com/_/rabbitmq) to start a local cluster
```shell
docker run -d --rm --name rabbitmq --hostname test -p 15672:15672 -p 5672:5672 rabbitmq:3-management
```
2. Install the subproject in editable more.
```shell
pip install -e ./plugins/rmq_io
```
3. To run the tests locally, you need to execute the following command:
```shell
python -m unittest discover .\plugins\rmq_io\test\ -p "*.py"
```
4. Delete cluster
```shell
docker stop rabbitmq
```

# Build

Before you build, you will need to install the "build" package:

```shell
pip install build
```

Then from the subproject's root, you will need to execute the following command:

```shell
python -m build
```

It will create the source distribution and the wheel file in the "dist" folder.

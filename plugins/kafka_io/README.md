# Description

This package contains the Kafka implementation of the Input-/OutputPortPlugin 
interface of *pypz*. It enables the operators to send and receive data records
in a real streaming fashion.

Check the [documentation](https://lazlowa.github.io/pypz-python/plugins/kafka_io.html) for
more details.

# Install

The python artifact is hosted on https://pypi.org/, so you can install
it via pip:

```shell
pip install pypz-kafka-io
```

If you want to work on it locally, then you should install in editable mode:

```shell
pip install -e ./plugins/kafka_io
```

# Test

You can run tests against an existing Kafka cluster, however, it is
more convenient and safe to use a local test cluster.


1. Install the subproject in editable more.
```shell
pip install -e ./plugins/kafka_io
```
2. Run the [confluent image](https://hub.docker.com/r/confluentinc/confluent-local) to start a local cluster
```shell
docker run --name kafka-test-cluster -d --rm -p 9092:9092 confluentinc/confluent-local
```
3. To run the tests locally, you need to execute the following command:
```shell
python -m unittest discover .\plugins\kafka_io\test\ -p "*.py"
```
4. Delete cluster
```shell
docker stop kafka-test-cluster
```

## Known issues

Due to the fact that the test execution is sometimes faster than the Kafka
cluster, there might be failed tests. Although there are already several
delays inserted into the test code, it still can happen. Try to rerun the
test and give a notification to the maintainer(s). Thank you.

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

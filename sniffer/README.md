# Description

This package contains the experimental implementation of the IO Sniffer.
The Sniffer allows you to visualize the control plane of the IO ports of
the operators, so you can have a better understanding, what happens during
the execution of your pipeline.

Check the [documentation](https://lazlowa.github.io/pypz-python/sniffer/overview.html) for
more details.

# Install

The python artifact is hosted on https://pypi.org/, so you can install
it via pip:

```shell
pip install pypz-io-sniffer
```

If you want to work on it locally, then you should install in editable mode:

```shell
pip install -e ./sniffer
```

# Test

Before you run the tests, you need to install the subproject in editable mode.
To run the tests locally, you need to execute the following command:

```shell
python -m unittest discover .\sniffer\test\ -p "*.py"
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

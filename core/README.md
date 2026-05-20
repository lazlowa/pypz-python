# Description

This package contains all the necessary core components to realize the
functionalities of *pypz*.

Check the [documentation](https://lazlowa.github.io/pypz-python/index.html) for
more details.

# Install

The python artifact is hosted on https://pypi.org/, so you can install
it via pip:

```shell
pip install pypz-core
```

If you want to work on it locally, then you should install in editable mode:

```shell
pip install -e ./core
```

# Test

Before you run the tests, you need to install the subproject in editable mode.
To run the tests locally, you need to execute the following command:

```shell
python -m unittest discover .\core\test\ -p "*.py"
```

# Build

Before you build, you will need to install the "build" package:

```shell
pip install build
```

Optionally, you might want to set a version for local testing. Using setuptools-scm, you can
pretend a version via setting the following environment variable:

```shell
# Windows
$env:SETUPTOOLS_SCM_PRETEND_VERSION_FOR_PYPZ_CORE="v1.2.3"

# Linux
export SETUPTOOLS_SCM_PRETEND_VERSION_FOR_PYPZ_CORE="v1.2.3"
```

Then from the subproject's root, you will need to execute the following command:

```shell
python -m build
```

It will create the source distribution and the wheel file in the "dist" folder.

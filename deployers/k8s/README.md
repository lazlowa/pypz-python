# Description

This package contains the Kubernetes implementation of the deployer interface
of *pypz*. It enables the deployment of the pipelines to a Kubernetes cluster.

Check the [documentation](https://lazlowa.github.io/pypz-python/deployers/kubernetes.html) for
more details.

# Install

The python artifact is hosted on https://pypi.org/, so you can install
it via pip:

```shell
pip install pypz-k8s-deployer
```

If you want to work on it locally, then you should install in editable mode:

```shell
pip install -e ./deployers/k8s
```

# Test

You can run tests against an existing Kubernetes cluster, however, it is
more convenient and safe to use [Kind](https://kind.sigs.k8s.io/) for this
purpose. 

1. Follow the instructions to install Kind. Note that Kind prepares the
required config file **~/.kube/config**. If you work with your own cluster,
make sure, you have this file already in place.
2. Install the subproject in editable more.
```shell
pip install -e ./deployers/k8s
```
3. Start the cluster
```shell
kind create cluster
```
4. Build the test images (test/resources)
```shell
docker build -t pypz-test-image .
```
5. Load the test image onto the Kind node
```shell
kind load docker-image pypz-test-image
```
6. To run the tests locally, you need to execute the following command:
```shell
python -m unittest discover .\deployers\k8s\test\ -p "*.py"
```
7. Delete cluster
```shell
kind delete cluster
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

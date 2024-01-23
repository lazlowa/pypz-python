# Local testing

## Requirements
- go - https://go.dev/doc/install
- kind - https://kind.sigs.k8s.io/

It requires go to be installed

## Set up the environment
1. Start the cluster
```shell
kind create cluster
```
2. Buid the test images (test/resources)
```shell
docker build -t pypz-test-image .
```
3. Load the test image onto the kind node
```shell
kind load docker-image pypz-test-image
```

## Clean up the environment
1. Delete cluster
```shell
kind delete cluster
```
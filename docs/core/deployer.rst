Deployer
========

*pypz* provides a deployer interface :class:`pypz.deployers.base.Deployer`
that can be used to implement different technologies.

It is expected to implement the abstract methods with the technology of your choice.

Check the class and method descriptions for more details.

.. note::
   *pypz* ships with an implementation for Kubernetes (:class:`pypz.deployers.k8s.KubernetesDeployer`)

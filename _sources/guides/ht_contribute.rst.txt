.. _contributors_guide:

Contributor's Guide
===================

Every contribution is very much appreciated and credited.
Before you begin, please read carefully the following sections.

Before You Begin
----------------

It is important to understand that *pypz* is an extensible framework. Different
interfaces are provided to allow you to integrate your custom implementation
with *pypz*. These implementations shall be maintained separately from *pypz*'s
core. In other words, if you want to implement your own operator, plugin,
deployer or channels, you don't need to contribute to this project, but you
shall create your own repository for that. On the other hand, if you want
to improve *pypz*'s core functionality or the builtin assets (e.g., K8s
deployer or the Kafka IO port plugins), then you are at the right place.

If you think that your implementation should be part of the base package
then contact me.

Code of conduct
---------------

By contributing to this project you agree and abide the terms and
conditions of the `Contributor Covenant Code of Conduct <https://github.com/lazlowa/pypz-python/blob/main/COC.md>`_.

Contributor License Agreement
-----------------------------

Read and understand the `Contributor License Agreement <https://github.com/lazlowa/pypz-python/blob/main/CLA.md>`_

Workflow
--------

Fork the Repository
+++++++++++++++++++

Setting up the Development
++++++++++++++++++++++++++

Follow the :ref:`Developer's Guide <developers_guide>` to set up your development environment.

Prepare and Submit the Pull Request
+++++++++++++++++++++++++++++++++++

Requirements
------------

styling
+++++++

Unit Tests
++++++++++

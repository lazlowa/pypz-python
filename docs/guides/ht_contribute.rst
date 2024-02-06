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

.. graphviz::
   :name: Contribution Workflow
   :align: center

   digraph state_machine {
       size="10,20";
       ratio="auto";
       rankdir="LR";
       graph [fontname="Verdana", fontsize="28"];
       margin="0.0,0.5";

       fork [shape=rect, label="Fork pypz-python/main"];
       setup [shape=rect, label="Setup the development"];
       implement [shape=rect, label="Implement"];
       pr [shape=rect, label="Prepare PR"];
       review [shape=rect, label="PR Review"];

       fork -> setup;
       setup -> implement;
       implement -> pr;
       pr -> review;
   }


Fork the Repository
+++++++++++++++++++

`Fork <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo>`_
the `pypz repository <https://github.com/lazlowa/pypz-python>`_.

Setting up the Development
++++++++++++++++++++++++++

Follow the :ref:`Developer's Guide <developers_guide>` to set up your development environment.

Prepare the Pull Request
++++++++++++++++++++++++

1. Always make sure that your fork is synced with the main branch of *pypz*
2. It is advised to create your own development branch instead of committing your changes directly to the
   main branch
3. Implement your fix, feature etc.
4. Implement the necessary unittests
5. Run the corresponding tests
6. Rebase your fork, squash commits and resolve conflicts
7. Commit message shall have a clear description of the changes (emphasis on the answers to "why")

Pass PR Review
++++++++++++++

Given the availability of the maintainer(s), your PR will be reviewed as soon as possible. During the review,
both parties shall be open minded for feedbacks and comments. Everybody can have opinions or ideology,
but facts and proofs are preferred, so don't get irritated or offended, if there are comments or requests to
your PR.

It makes the review process much easier, if you provide your intentions behind your changes e.g., as
code docs.

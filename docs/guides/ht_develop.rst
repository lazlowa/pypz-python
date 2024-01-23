.. _developers_guide:

Developer's Guide
=================

Getting the Source Code
-----------------------

To get a local copy of the source code, you simply need to clone it using this command:

.. code-block:: shell

   git clone https://github.com/lazlowa/pypz-python.git

Project Structure
-----------------

As you can see, *pypz* follows the monorepo structure i.e., there is a root project called "pypz-python", which
contains several subprojects organized into their independent packages. Each subproject follows the
`src layout <https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/>`_ i.e., the packages
are organized into a "src" folder in the subproject.

.. note::
   In the following, you will see the term **"builtin"** used several times. It refers to interface implementations
   that are considered as part of *pypz*'s base project. These implementations are maintained by this project. This
   does not limit you to have your own implementations of the interfaces, however those shall be maintained by you
   in your own repository.

   If you think that your implementation should be part of the base project, then please contact the maintainers.

**Folder structure:**

- **core**, contains the core components and features that makes *pypz* work
- **deployers**, logical grouping of subprojects that are implementing the deployer interface
- **plugins**, logical grouping of subprojects that are implementing the plugin interfaces
- **sniffer**, contains the sniffer subproject
- **docs**, contains the documentation
- **.docker**, contains the Dockerfiles and necessary image resources
- **.github**, contains the github actions scripts

.. important::
   Notice that the modules are organized in namespace packages. This enables us to reuse package names in different
   subprojects. For example, the deployer interface is provided by the module "pypz.deployers.base", the Kubernetes
   implementation is provided by the module "pypz.deployers.k8s".

Work in the Project
-------------------

Due to the project structure, if you want to test/run components of *pypz*, you need to install the related *pypz*
dependencies in editable mode. You can achieve this via pip. For example, given you are in the project root, you
can execute the following command to install the subprojects:

.. code-block:: shell

   pip install -e ./core/
   pip install -e ./deployers/k8s/
   pip install -e ./plugins/kafka_io/
   ...

Or you can install everything via the prepared
`requirements.txt <https://github.com/lazlowa/pypz-python/blob/main/requirements.txt>`_ file.

.. code-block:: shell

   pip install -r requirements.txt

Run Tests
---------

Different subprojects might have different steps to take, before you can run the tests. Please check the
corresponding README for more information.

.. _static_analysis:

Perform Static Analysis
-----------------------

.. note::
   All tests and static analysis will be performed upon creating a pull request. However, it is a good
   practice to perform these steps locally as well, before pushing.

To perform the following actions, you need to install the optional dependencies for the project.
The ``requirements.txt`` file already contains the information to install the monorepo in editable
mode incl. the optional dependencies for static analysis.

.. code-block:: shell

   pip install -r requirements.txt

Test Coverage
+++++++++++++

To evaluate test coverage, the python package `coverage <https://coverage.readthedocs.io/en/7.4.1/>`_ is used.
To perform the tests incl. coverage analysis, you need to execute the following command:

.. code-block:: shell

   coverage run -m unittest discover core/test -p "*.py"

.. note::
   Notice that this will execute the tests on the core subproject. To have coverage reports on other
   subprojects, you need to adapt the command accordingly.

Once the test ran, you can create the report with the following command:

.. code-block:: shell

   coverage report

Please refer to the documentation of `coverage <https://coverage.readthedocs.io/en/7.4.1/>`_ for more options.

Linting
+++++++

For linting purposes `flake8 <https://flake8.pycqa.org/en/latest/>`_ is used. You can find the
corresponding configuration in `setup.cfg <https://github.com/lazlowa/pypz-python/blob/main/setup.cfg>`_.

To scan the entire repository with `flake8 <https://flake8.pycqa.org/en/latest/>`_, execute the following command:

.. code-block:: shell

   flake8 $(git ls-files *.py)

To scan only the modified files with `flake8 <https://flake8.pycqa.org/en/latest/>`_,
execute the following command (at project's root):

.. code-block:: shell

   flake8 $(git diff --name-only main  ./*.py)

Type Checking
+++++++++++++

It is desired to some extent to check type correctness throughout the entire project. For that purpose
`mypy <https://mypy-lang.org/>`_ is used. It helps to look at python like it would be a static typed
language. You can find mypy's configuration in the
`pyproject.toml <https://github.com/lazlowa/pypz-python/blob/main/pyproject.toml>`_ file.

*pypz* configures mypy to always check the entire project instead of only the modified files. This
is necessary, since your changes can cause downstream issues. To perform the type checking, execute the
following command from project's root:

.. code-block:: shell

   mypy .

Build
-----

To build your project, you will need the ``build`` tool first:

.. code-block:: shell

   python -m pip install build

Then you can build your project by invoking the following command from the subproject's root

.. code-block:: shell

   python -m build <SUBPROJECT_LOCATION>

This command will create the sdist and wheel into the dist folder.

Build the Documentation
-----------------------

Since the documentation is generated with Sphinx and refers to all subprojects, you need to make sure
that every subproject is installed.

.. code-block:: shell

   pip install -r requirements.txt

Additionally, you need to install the dependencies for the Sphinx extensions:

.. code-block:: shell

   pip install -r ./docs/requirements.txt

As last, you need to install `GraphViz <https://graphviz.org/>`_, since its features are used
by Sphinx to generate images in the documentation.

The indexes are regenerated in the publish action. However, if you want to rebuild them locally,
you can do it with the following commands from the docs folder:

.. code-block:: shell

   sphinx-apidoc --implicit-namespaces -e -f --private -o ./indexes/core ../core/src/pypz/
   sphinx-apidoc --implicit-namespaces -e -f --private -o ./indexes/sniffer ../sniffer/src/pypz/
   sphinx-apidoc --implicit-namespaces -e -f --private -o ./indexes/plugins/kafka_io ../plugins/kafka_io/src/pypz/
   sphinx-apidoc --implicit-namespaces -e -f --private -o ./indexes/deployers/k8s ../deployers/k8s/src/pypz/

.. warning::
   Since the index is generated automatically with implicit namespaces, there are some known issues with the
   documentation build:

   - multiple .rst files of the common namespaces are generated, which causes the following warning
     ``WARNING: duplicate object description [...] use :no-index: for one of them``
   - the generated module.rst files are not included in any toctree causing the warning
     ``WARNING: document isn't included in any toctree``

You can build the documentation with the following command:

.. code-block:: shell

   sphinx-build ./docs ./docs/_build

Then you can open the generated index.html under ./docs/_build/index.html.

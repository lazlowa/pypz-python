from pypz.version import PROJECT_VERSION

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pypz"
copyright = "2024, Laszlo Anka"
author = "Laszlo Anka"
version = PROJECT_VERSION

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.inheritance_diagram",
    "sphinx.ext.graphviz",
    "sphinx_copybutton",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

inheritance_graph_attrs = dict(rankdir="TB", ratio="fill", size='"16.0, 10.0"')
inheritance_node_attrs = dict(
    shape="ellipse", fontsize=32, color="dodgerblue1", style="filled", height=0.8
)

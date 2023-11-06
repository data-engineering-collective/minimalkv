import inspect
import os
import sys

from sphinx.ext import apidoc

from minimalkv import __version__ as version

release = version

sys.path.append("../")

package = "minimalkv"
html_theme = "alabaster"

__location__ = os.path.join(
    os.getcwd(),
    os.path.dirname(inspect.getfile(inspect.currentframe())),  # type: ignore
)


# Generate module references
output_dir = os.path.abspath(os.path.join(__location__, "../docs/_rst"))
module_dir = os.path.abspath(os.path.join(__location__, "..", package))

apidoc_parameters = ["-f", "-e", "-o", output_dir, module_dir]
apidoc.main(apidoc_parameters)

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.intersphinx",
]

templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"

# General information about the project.
project = "minimalkv"
copyright = "2011-2021, The minimalkv contributors"


exclude_trees = ["_build"]

pygments_style = "sphinx"

intersphinx_mapping = {
    "http://docs.python.org/": None,
    "http://docs.pythonboto.org/en/latest/": None,
    "http://sendapatch.se/projects/pylibmc/": None,
    "http://www.sqlalchemy.org/docs/": None,
    "http://redis-py.readthedocs.org/en/latest/": None,
}

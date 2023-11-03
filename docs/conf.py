import inspect
import os
import re
import sys

from sphinx.ext import apidoc

sys.path.append("../")

package = "minimalkv"
html_theme = "alabaster"

__location__ = os.path.join(
    os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe()))  # type: ignore
)


def simplify_version(version):
    """
    Simplifies the version string to only include the major.minor.patch components.

    Example:
    '1.8.2.post0+g476bc9e.d20231103' -> '1.8.2'
    """
    match = re.match(r"^(\d+\.\d+\.\d+)(?:\.post\d+)?", version)
    return match.group(1) if match else version


try:
    import minimalkv

    version = simplify_version(minimalkv.__version__)
except ImportError:
    import pkg_resources

    version = simplify_version(pkg_resources.get_distribution("minimalkv").version)

print(f"Building docs for version: {version}")

# The version info is fetched programmatically. It acts as replacement for
# |version| and |release|, it is also used in various other places throughout
# the built documents.
#
# major.minor.patch

release = version

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

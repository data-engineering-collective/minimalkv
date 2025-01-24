# Run in a temporary pixi env to check whether the installed package can be imported and
# used to create an hfs store that doesn't require any additional dependencies.

# This check is useful because we don't want to have hard-coded dependencies in the package
# apart from "uritools". Everything else should be optional and only raise an ImportError
# if the user tries to use a specific store backend, e.g. s3://.

whl_file=$(ls dist/*.whl)
pip install $whl_file
python -c 'from minimalkv import get_store; get_store("hfs", path=".")'

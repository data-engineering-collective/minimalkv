Development
***********

Github
======
All official development of the library takes place on `GitHub
<https://github.com/data-engineering-collective/minimalkv>`_. Comments, bug reports and patches are
usually welcome, if you have any questions regarding the library, you can message
the maintainers there as well.


Unit tests
==========

The test suite runs locally using Docker. The cloud stores (s3, GCS, Azure)
are tested against emulators.
To run the tests, setup the Python environment and execute:
::

    docker-compose up -d
    pytest -n auto --dist loadfile
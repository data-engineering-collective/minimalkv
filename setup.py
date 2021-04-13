#!/usr/bin/env python

from os import path

from setuptools import find_packages, setup


here = path.abspath(path.dirname(__file__))

with open("README.rst") as f:
    long_description = f.read()

setup(
    name="minimalkv",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description=("A key-value storage for binary data, support many " "backends."),
    long_description=long_description,
    author="Data Engineering Collective",
    author_email="minimalkv@uwekorn.com",
    url="https://github.com/data-engineering-collective/minimalkv",
    license="MIT",
    packages=find_packages(exclude=["test"]),
    install_requires=[],
    python_requires=">=3.7",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
    ],
)

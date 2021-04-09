#!/usr/bin/env python
# coding=utf8

import os

from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="minimalkv",
    version="0.0.1",
    description=("A key-value storage for binary data, support many " "backends."),
    long_description=read("README.rst"),
    author="Data Engineering Collective",
    author_email="minimalkv@uwekorn.com",
    url="https://github.com/data-engineering-collective/minimalkv",
    license="MIT",
    packages=find_packages(exclude=["test"]),
    install_requires=[],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)

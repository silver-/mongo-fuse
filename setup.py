#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='mongofuse',
    version='0.1',
    description="FUSE interface for MongoDB ",
    packages=find_packages(exclude=['ez_setup']),
    install_requires=[
        'fusepy',
        'pymongo'
    ],
    entry_points = {
        'console_scripts': [
            'mongofuse = mongofuse.mongofuse:main',
        ]
    }
)

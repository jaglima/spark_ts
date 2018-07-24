# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='spark_ts',
    version='0.1.0',
    description='Pyspark package supporting climate computing',
    long_description=readme,
    author='Jesse Lima',
    author_email='jesseamerico@gmail.com',
    url='https://github.com/jaglima/climaspark',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)


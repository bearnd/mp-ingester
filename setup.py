#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.md') as history_file:
    history = history_file.read()

requirements = [
]

setup_requirements = [
    'pytest-runner',
]

test_requirements = [
    'pytest',
]

setup(
    name='mp_ingester',
    version='0.1.1',
    description="MedlinePlus XML dump parser and SQL ingester.",
    long_description=readme + '\n\n' + history,
    author="Adamos Kyriakou",
    author_email='adam@bearnd.io',
    url='https://github.com/somada141/mp_ingester',
    packages=find_packages(include=['mp_ingester']),
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='mp_ingester',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)

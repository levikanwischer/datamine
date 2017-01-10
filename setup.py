# -*- coding: utf-8 -*-

"""DBBAPI 2.0 inspired DataMine API interface for Upsight.com."""

import codecs
import os
import re
from setuptools import setup, find_packages


NAME = 'datamine'
HERE = os.path.dirname(os.path.abspath(__file__))


def read(filename):
    """Read file contents from 'this' directory."""
    filepath = os.path.join(HERE, filename)

    if not os.path.isfile(filepath):
        raise FileNotFoundError('%s missing from `%s`.' % (filename, NAME))

    with codecs.open(filepath, encoding='utf-8') as infile:
        contents = infile.read()

    return contents


def package_init():
    """Load package init contents into memory."""
    filepath = os.path.join(NAME, '__init__.py')
    contents = read(filepath)
    return contents


CONTENTS = package_init()


def package_metadata(attribute, contents=CONTENTS):
    """Extract metadata from contents."""
    regex = r'''__%s__\s*=\s*['\"\[]([^'\"]*)['\"\]]''' % attribute

    match = re.search(regex, contents, re.M)
    result = match.group(1) if match else None

    if result is None:
        raise IndexError('`__%s__` missing from package contents.' % attribute)

    return result


def package_description(*filenames):
    """Merge file contents into single 'long description'."""
    length = len(__doc__)
    description = '%s\n%s\n' % (__doc__, '-' * length)

    for filename in filenames:
        contents = read(filename)
        description = '%s\n%s' % (description, contents)

    return description


setup(
    name=NAME,

    version=package_metadata('version'),
    license=package_metadata('license'),

    author=package_metadata('author'),

    maintainer=package_metadata('maintainer'),
    maintainer_email=package_metadata('email'),

    description=__doc__,
    long_description=package_description('README.rst'),
    keywords=['datamine', ],

    packages=find_packages(
    ),

    install_requires=[
        'requests',
    ],
    extras_require={
        'dev': ['flake8', 'pylint', ]
    },

    entry_points={
        'console_scripts': [
        ]
    },

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)

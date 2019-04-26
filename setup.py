import os
import subprocess

import sys
from setuptools import Command, setup

import databasin


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    long_description = f.read()


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)


setup(
    name='python-databasin',
    description='A client library for Data Basin (http://databasin.org)',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Conservation Biology Institute',
    keywords='data basin',
    version=databasin.__version__,
    packages=['databasin'],
    install_requires=['python-dateutil', 'six', 'requests', 'restle==0.5.0'],
    url='https://github.com/consbio/python-databasin',
    license='BSD',
    tests_require=['pytest', 'requests-mock>=0.7.0', 'mock', 'django>=1.11.20,<1.12'],
    cmdclass={'test': PyTest}
)

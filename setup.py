import subprocess

import sys
from setuptools import Command, setup

import databasin


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
    author='Conservation Biology Institute',
    keywords='data basin',
    version=databasin.__version__,
    packages=['databasin'],
    install_requires=['six', 'requests', 'restle'],
    url='https://github.com/consbio/python-databasin',
    license='BSD',
    tests_require=['pytest', 'requests-mock>=0.7.0', 'mock'],
    cmdclass={'test': PyTest}
)

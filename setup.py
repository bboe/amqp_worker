#!/usr/bin/env python
import os
import re
from setuptools import setup

MODULE_NAME = 'amqp_worker'

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
VERSION = re.search("__version__ = '([^']+)'",
                    open('{0}.py'.format(MODULE_NAME)).read()).group(1)

setup(name=MODULE_NAME,
      author='Bryce Boe',
      author_email='bbzbryce@gmail.com',
      classifiers=['Intended Audience :: Developers',
                   'License :: OSI Approved :: BSD License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 3'],
      description=('A python module for writing workers (daemons) triggered '
                   'from amqp jobs.'),
      install_requires=['pika>=0.9.6', 'python-daemon>=1.5.5'],
      license='Simplified BSD License',
      long_description=README,
      py_modules=[MODULE_NAME],
      url='https://github.com/bboe/amqp_worker',
      version=VERSION)

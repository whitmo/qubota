from setuptools import setup
from setuptools import find_packages
import sys
import os

version = '0.1'

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()


setup(name='awsq',
      version=version,
      description="A AWS based job queue",
      long_description=README + CHANGES,
      classifiers=[],
      keywords='',
      author='whit',
      author_email='',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'cliff-tablib',
          'boto',
          'PyYAML',
          'cliff',
          'stuf',
          'gevent==1.0rc1'
          ],
      entry_points="""
      [console_scripts]
      awsq=awsq.cli:main
      [awsq.cli]
      up=awsq.cli:QUp
      down=awsq.cli:QDown
      nq=awsq.cli:EnqueueJob
      jl=awsq.cli:ShowJobs
      joblist=awsq.cli:ShowJobs
      ml=awsq.cli:ShowMsgs
      msglist=awsq.cli:ShowMsgs
      """,
      )


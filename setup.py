from setuptools import setup
from setuptools import find_packages
import os

version = '0.1'

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()


setup(name='qubota',
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
          'PyYAML',
          'boto',
          'cliff',
          'cliff-tablib',
          'stuf',
          'path.py',
          'requests'
          ],
      entry_points="""
      [console_scripts]
      qubota=qubota.cli:main
      qb=qubota.cli:main
      wmm=qubota.wmm:main

      [qubota.cli]
      up=qubota.cli:QUp
      down=qubota.cli:QDown
      nq=qubota.cli:EnqueueJob
      jl=qubota.cli:ShowJobs
      joblist=qubota.cli:ShowJobs
      ml=qubota.cli:ShowMsgs
      msglist=qubota.cli:ShowMsgs
      drain=qubota.cli:Drain
      drone=qubota.cli:Drone
      nm=qubota.cli:NoiseMaker
      """,
      )







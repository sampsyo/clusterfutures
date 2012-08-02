import os
from setuptools import setup

def _read(fn):
    path = os.path.join(os.path.dirname(__file__), fn)
    return open(path).read()

setup(name='clusterfutures',
      version='0.2',
      description='futures for remote execution on clusters',
      author='Adrian Sampson',
      author_email='asampson@cs.washington.edu',
      url='https://github.com/sampsyo/clusterfutures',
      license='MIT',
      platforms='ALL',
      long_description=_read('README.rst'),

      packages=['cfut'],
      install_requires=[
          'cloud',
          'futures',
      ],

      classifiers=[
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
      ],
)

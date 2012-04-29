Cluster Futures
===============

This module provides a Python `concurrent.futures`_ executor that lets you run
functions on remote systems in your `Condor`_ or `Slurm`_ cluster. Stop worrying
about writing job files, scattering/gathering, and serialization---this module
does it all for you.

It uses `PiCloud`_'s advanced pickler to even allow (most) closures to be used
transparently, so you're not limited to "pure" functions.

Depends on ``concurrent.futures`` and ``cloud``, so just run ``pip install
futures cloud`` to get up and running.

See ``example_*.py`` for example usage. The easiest way to get started is to
ignore the fact that futures are being used at all and just use the provided
``map`` function, which behaves like `itertools.imap`_ but transparently
distributes your work across the cluster.

.. _concurrent.futures:
    http://docs.python.org/dev/library/concurrent.futures.html
.. _Condor: http://www.cs.wisc.edu/condor/
.. _PiCloud: http://www.picloud.com/
.. _itertools.imap: http://docs.python.org/library/itertools.html#itertools.imap
.. _Slurm: https://computing.llnl.gov/linux/slurm/

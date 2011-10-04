Cluster Futures
===============

This module provides a Python [concurrent.futures][futures] executor that lets
you run functions on remote systems in your [Condor][] cluster. Stop worrying
about writing job files, scattering/gathering, and serialization---this module
does it all for you.

It uses [PiCloud][]'s advanced pickler to even allow (most) closures to be used
transparently, so you're not limited to "pure" functions.

Depends on `concurrent.futures` and `cloud`, so just run `pip install futures
cloud` to get up and running. See `example.py` for example usage.

[futures]: http://docs.python.org/dev/library/concurrent.futures.html
[Condor]: http://www.cs.wisc.edu/condor/
[PiCloud]: http://www.picloud.com/

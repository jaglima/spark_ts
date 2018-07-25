Spark_ts - Pyspark data wrangling package
=========================================


This module is a free Python package tools supporting Pyspark for time series
treatment of big data in Climate research.

This README file is intended primarily for people interested in working
with the spark_ts source code available in dd
https://github.com/jaglima/spark_ts

This package is open source software made available under GNU license.
Please see the LICENSE file for further details.

For the impatient
=================

Python 2.7.9 onwards, and Python 3.4 onwards, include the package management
system "pip" which should allow you to install this package with::

    pip install numpy
    pip install spark_ts

Otherwise you may have to build and install Spark_ts. Download and unzip the
source code, go to this directory at the command line, and type::

    python setup.py build
    python setup.py test
    sudo python setup.py install

Here you can replace ``python`` with a specific version, e.g. ``python3.5``.


Python Requirements
===================

We currently recommend using Python 3.5 from http://www.python.org

Dependencies
============

Spark_ts requires basically Pyspark SQL packges.  

- NumPy, see http://www.numpy.org (optional, but strongly recommended)
  This package is only used in the computationally-oriented modules.
  It is required for Bio.Cluster, Bio.PDB and a few other modules.  If you
  think you might need these modules, then please install NumPy first BEFORE
  installing Biopython. The older Numeric library is no longer supported in
  Biopython.

- matplotlib, see http://matplotlib.org/ (optional)
  Bio.Phylo uses this package to plot phylogenetic trees. As with ReportLab,
  you can install this at any time to enable the plotting functionality.


Installation
============

First, **make sure that Pyspark is installed correctly**. Second, we
recommend you install NumPy (see above). Then install Spark_ts.

Installation from source should be as simple as going to the spark_ts
source code directory, and typing::

    python setup.py build
    python setup.py test
    sudo python setup.py install


If you need to do additional configuration, e.g. changing the base
directory, please type `python setup.py`, or see the documentation here:


Testing
=======

Biopython includes a suite of regression tests to check if everything is
running correctly. To run the tests, go to the biopython source code
directory and type::

    python setup.py test

Do not panic if you see messages warning of skipped tests::

    test_DocSQL ... skipping. Install MySQLdb if you want to use Bio.DocSQL.

This most likely means that a package is not installed.  You can
ignore this if it occurs in the tests for a module that you were not
planning on using.  If you did want to use that module, please install
the required dependency and re-run the tests.

There is more testing information in the Biopython Tutorial & Cookbook.


Experimental code
=================

This package is in experimental code phase. Use it at your own risk.

Distribution Structure
======================

- README      -- This file.
- LICENSE     -- What you can do with the code.
- MANIFEST.in -- Tells distutils what files to distribute.
- setup.py    -- Installation file.
- spark_ts/   -- The main code base code.
- Doc/        -- Documentation.
- Tests/      -- Testing code including sample data files.

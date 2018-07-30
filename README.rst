Spark_ts - Pyspark data wrangling package
=========================================


This module is a free Python package tools supporting Pyspark for time series
treatment and exploration of big data in Climate research.

This README file is intended primarily for people interested in working
with the spark_ts source code available in 
https://github.com/jaglima/spark_ts

This package is open source software made available under GNU license.
Please see the LICENSE file for further details.

For the impatient
=================

You may have to build and install Spark_ts. Download and unzip the
source code, go to this directory at the command line, and type::

    python setup.py build
    sudo python setup.py install

You can replace ``python`` with a specific version, e.g. ``python3.5``.

By using pip (available Python 2.7.9 onwards, and Python 3.4 onwards) to install this package, let::

    cd spark_ts/
    pip install .

Python Requirements
===================

We currently recommend using Python 3.5 from http://www.python.org

Dependencies
============

Spark_ts requires basically Pyspark SQL packges.  

- For testing suite you must intall Pandas package.


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

In development

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

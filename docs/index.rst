FLUXNET Shuttle Library Documentation
=====================================

Welcome to FLUXNET Shuttle Library's documentation!

FLUXNET Shuttle Library is a Python library for FLUXNET shuttle operations providing
core functionality for data processing and analysis.

Features
--------

- Core data processing utilities for FLUXNET datasets
- Pydantic models for data validation
- Type-safe interfaces for data manipulation
- Comprehensive test coverage

Installation
------------

From PyPI (when published)::

    pip install fluxnet-shuttle-lib

For Development::

    git clone https://github.com/AMF-FLX/fluxnet-shuttle-lib.git
    cd fluxnet-shuttle-lib
    pip install -e .[dev,docs]

Quick Start
-----------

.. code-block:: python

    from fluxnet_shuttle_lib import main

    # Run the main function
    main()

API Reference
=============

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules

Main Module
-----------

.. automodule:: fluxnet_shuttle_lib.main
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
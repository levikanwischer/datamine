datamine
========

DBAPI 2.0 inspired DataMine API interface for Upsight.com.


Usage
-----
.. code-block:: python

    >>> from datamine import DataMine
    >>> with DataMine('YOUR_USERNAME', 'YOUR_PASSWORD') as datamine:
    >>>     datamine.execute('YOUR_QUERY_STRING')
    >>>     datamine.download('YOUR_DOWNLOAD_FILENAME.csv')


Installation
------------
.. code-block:: bash

    $ pip install git+https://github.com/levikanwischer/datamine.git

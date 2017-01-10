datamine
========

DBAPI 2.0 inspired DataMine API interface for Upsight.com.


Usage
-----
.. code-block:: python

    >>> import datamine
    >>> with DataMine('YOUR_USERNAME', 'YOUR_PASSWORD') as datamine:
    >>>     datamine.execute('YOUR_QUERY_STRING')
    >>>     datamine.download('YOUR_DOWNLOAD_FILENAME.csv')


Installation
------------
.. code-block:: bash

    $ pip install git+https://github.com/levikanwischer/datamine.git


Requirements
------------
N/A


Contribute
----------
#. Check/Open Issue for related topics of change
#. Fork/Clone/Branch repo and make discussed/desired changes
#. Add tests and document code w/ numpy formatting
#. Open Pull Request and notify author

# -*- coding: utf-8 -*-

"""This module contains a DBAPI2.0 like interface for querying DataMine."""

import codecs
import csv
import logging
import os
import time
from collections import OrderedDict

import requests
from requests.auth import HTTPBasicAuth

# Suppress excessive logging messages from `requests`
logging.getLogger('requests').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


class DataMine(object):
    """DBAPI 2.0 inspired DataMine API interface for Upsight.com.

    Parameters
    ----------
    username : str
        Username for login to upsight.com.
    password : str
        Password for login to upsight.com.
    server : str, optional (default=_LEGACY_SERVER)
        Base DataMine API url for upsight.com.

    Attributes
    ----------
    server : str
        Base DataMine API url for upsight.com.
    connection : object <requests.Session>
        Current session object.
    cursor : object <requests.Response>
        Streaming data response from latest request.
    columns : array-like <tuple>, (default=None)
        Column names for most recent query execution.
    record : array-like <dict>, optional (default=None)
        Column names & values for current cursor object.

    Methods
    -------
    execute(query="select * from table limit 10;")
        Execute query against cursor.
    fetchone()
        Retrieve next record from cursor.
    fetchmany(rows=5)
        Retrieve next n records from cursor.
    fetchall()
        Retrieve all records from cursor.
    download(filename='example.csv')
        Download cursor results to filename.

    Examples
    --------
    >>> username = 'YOUR_UPSIGHT_USERNAME'
    >>> password = 'YOUR_UPSIGHT_PASSWORD'
    >>> query = "select item, count(1) as inv from fruit group by fruit;"
    >>>
    >>> with DataMine(username, password) as datamine:
    >>>     datamine.execute(query)
    >>>     row = datamine.fetchone()
    >>>     while row is not None:
    >>>         print(row)
    >>>         row = datamine.fetchone()
    OrderedDict([('ITEM', 'apples'), ('INV', '1')])
    OrderedDict([('ITEM', 'bananas'), ('INV', '2')])
    OrderedDict([('ITEM', 'kiwi'), ('INV', '12')])

    """

    _LEGACY_SERVER = r'https://analytics.upsight.com/dashboard/datamine2'
    SLEEP_DURATION_SECS = 15

    def __init__(self, username, password, server=_LEGACY_SERVER):
        self.server = server
        self.connection = requests.Session()
        self.connection.auth = HTTPBasicAuth(username, password)

        self.cursor = None
        self.columns = None
        self.record = None

    def _check_request_reason(self, request=None, server=None):
        """Check server status from upsight.com.

        Parameters
        ----------
        request : object <requests.Request>, optional (default=None)
            Response object from calling request on ``requests`` package.
        server : string, optional (default=self.server)
            Server URL to specific Upsight.com's DataMine endpoint.

        Returns
        -------
        reason : string
            Server response (reasoning) as a status name.

        """
        server = server or self.server
        request = request or self.connection.get(server)
        reason = request.reason
        return reason

    def __enter__(self):
        """Context manager entrance method."""
        attempts, reason = 5, self._check_request_reason()

        while attempts and reason != 'OK':

            if reason == 'FORBIDDEN':
                msg_ = 'User access denied, update credentials.'
                raise requests.exceptions.ConnectionError(msg_)

            attempts -= 1
            time.sleep(5)

            reason = self._check_request_reason()

        if not attempts or reason != 'OK':
            msg_ = 'Unable to successfully connect, exiting.'
            raise requests.exceptions.ConnectionError(msg_)

        return self

    def __exit__(self, type_, value_, trackback_):
        """Context manager exit method."""
        self.connection.close()

    def _columns(self):
        """Extract & format column names from initial row.

        Returns
        -------
        columns : array-like <tuple>, (default=None)
            Column names from cursor.

        """
        if self.cursor is None:
            self.columns = None
            return self.columns

        if isinstance(self.columns, tuple):
            return self.columns

        columns = next(self.cursor)

        if columns is None:
            self.columns = None
            return self.columns

        columns = columns.decode('ascii', 'ignore')
        columns = columns.upper()
        columns = columns.split(',')
        self.columns = tuple(column.strip() for column in columns)

        return self.columns

    def execute(self, query, attempts=3):
        """Execute query against cursor.

        Parameters
        ----------
        query : str
            Fully parameterized query string.
        attempts : int, optional (default=3)
            Max number of retries on execute failure.

        Returns
        -------
        statuscodes : array-like <list>
            List of invalid codes from query execution, empty if successful.

        """
        self.cursor = None
        self.columns = None
        self.record = None

        data = {'query': query}

        url = '%s/query/' % self.server

        statuscodes, posted = [], False
        while not posted and attempts > 0:
            request = self.connection.post(url, data=data)
            reason = self._check_request_reason(request=request)

            response = request.json()
            if reason == 'CREATED' and 'id' in response:
                statuscodes, posted = [], True
                queryid = response['id']

            else:
                statuscodes.append(reason)
                time.sleep(self.SLEEP_DURATION_SECS * 0.25)
                attempts -= 1

        statuscodes = list(set(statuscodes))

        if not posted and attempts <= 0:
            msg_ = 'Posting failed for: %s w/ %s' % (query, statuscodes)
            raise requests.exceptions.ConnectionError(msg_)

        url = '%s/%s/results' % (url, queryid)

        completed = False
        while not completed and attempts > 0:
            request = self.connection.get(url)
            reason = self._check_request_reason(request=request)

            if reason == 'OK':
                self.cursor = request.iter_lines()
                self._columns()
                completed = True

            elif reason != 'PROCESSING':
                statuscodes.append(reason)
                attempts -= 1

            if reason != 'OK' and attempts > 0:
                time.sleep(self.SLEEP_DURATION_SECS)

        statuscodes = list(set(statuscodes))

        if not completed and attempts <= 0:
            msg_ = 'Processing failed for: %s w/ %s' % (query, statuscodes)
            raise requests.exceptions.ConnectionError(msg_)

        return statuscodes

    def fetchone(self):
        """Retrieve next record from cursor.

        Raises
        ------
        IndexError
            If record length != columns length.

        Returns
        -------
        record : array-like <OrderedDict>
            Current record from cursor w/ column names.

        """
        columns = self._columns()
        if columns is None:
            self.record = None
            return self.record

        try:
            record = next(self.cursor)
        except StopIteration:
            record = None

        if record is None:
            self.record = None
            return self.record

        record = record.decode('ascii', 'ignore')
        record = record.replace(r'\N', r'')
        record = record.replace(r'\:', r':')
        record = record.replace(r'\,', r'$|$')
        record = record.split(',')
        record = tuple(column.replace(r'$|$', r',') for column in record)
        record = tuple(column.strip() for column in record)

        if len(record) != len(self.columns):
            # WARNING: Implicitly ignores "malformed" rows
            self.fetchone()

        self.record = OrderedDict(zip(self.columns, record))

        return self.record

    def fetchmany(self, rows=10):
        """Retrieve next n records from cursor.

        Parameters
        ----------
        rows : int, optional (default=10)
            Number of records/rows to be returned.

        Returns
        -------
        records : array-like <dict>
            Next n records from cursor w/ column names.

        """
        records = []

        record = self.fetchone()
        while record is not None and rows > 0:
            records.append(record)
            record = self.fetchone()
            rows -= 1

        return records

    def fetchall(self):
        """Retrieve all records from cursor.

        Returns
        -------
        records : array-like <dict>
            Remaining records from cursor w/ column names.

        """
        records = []

        record = self.fetchone()
        while record is not None:
            records.append(record)
            record = self.fetchone()

        return records

    def download(self, filename, fieldnames=True):
        """Download cursor results to filename.

        Parameters
        ----------
        filename : str
            Filename/path result to be downloaded to.
        fieldnames : boolean, optional (default=True)
            Flag for inclusion of column names in output.

        Raises
        ------
        OSError
            If dir of ``filename`` is not a valid path.
            If dir of ``filename`` is not writeable.
            If ``filename`` is not writeable.
        ValueError
            If ``columns`` is None.

        """
        parentname = os.path.dirname(filename)
        if not parentname:
            parentname = os.getcwd()

        if not os.path.isdir(parentname):
            msg_ = '%s is not a valid filepath.' % parentname
            raise OSError(msg_)

        writeable = os.access(parentname, os.W_OK)
        if not writeable:
            msg_ = '%s is not a writeable directory.' % parentname
            raise OSError(msg_)

        if os.path.isfile(filename):
            writeable = os.access(filename, os.W_OK)
            if not writeable:
                msg_ = '%s is not a writeable file.' % filename
                raise OSError(msg_)

        columns = self._columns()
        if columns is None:
            msg_ = 'Query response empty, cannot download to %s.' % filename
            raise ValueError(msg_)

        with codecs.open(filename, 'w', encoding='utf-8') as writefile:
            writer = csv.DictWriter(
                writefile,
                lineterminator='\n',
                fieldnames=self.columns
            )

            if fieldnames:
                writer.writeheader()

            record = self.fetchone()
            while record is not None:
                writer.writerow(record)
                record = self.fetchone()

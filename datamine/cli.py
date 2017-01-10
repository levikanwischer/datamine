# -*- coding: utf-8 -*-

"""This module contains a commandline interface for DataMine."""
from __future__ import print_function

import getpass

import click
from tabulate import tabulate

from datamine.core import DataMine


@click.group()
def main():
    """CLI for DataMine API (Upsight.com)."""
    pass


@main.command()
@click.option('--username', '-u', help='Username for Upsight.com.')
@click.option('--query', '-q', help='Query string to execute.')
@click.option('--rows', '-r', default=None, help='Number of rows to show.')
def show(username, query, rows):
    """Print n rows of query results to console."""
    password = getpass.getpass('Upsight Password: ')

    if not isinstance(rows, int):
        rows = None

    with DataMine(username, password) as datamine:
        datamine.execute(query)

        if rows is None:
            records = datamine.fetchall()
        else:
            records = datamine.fetchmany(rows)

    tabbed = tabulate(records, "keys", tablefmt='grid')
    print(tabbed)


@main.command()
@click.option('--username', '-u', help='Username for Upsight.com.')
@click.option('--query', '-q', help='Query string to execute.')
@click.option('--filename', '-f', help='File to download to.')
def download(username, query, filename):
    """Download query results to given filename."""
    password = getpass.getpass('Upsight Password: ')

    with DataMine(username, password) as datamine:
        datamine.execute(query)
        datamine.download(filename)

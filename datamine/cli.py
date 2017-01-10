# -*- coding: utf-8 -*-

"""This module contains a commandline interface for DataMine."""
from __future__ import print_function

import click
from tabulate import tabulate

from datamine.core import DataMine


@click.group()
def main():
    pass


@main.command()
@click.option('--username', '-u', help='Username for Upsight.com.')
@click.password_option()
@click.option('--query', '-q', help='Query string to execute.')
@click.option('--rows', '-r', default=None, help='Number of rows to show.')
def show(username, password, query, rows):
    """Print n rows of query results to console."""
    rows = -1 if rows is None else rows

    with Datamine(username, password) as datamine:
        datamine.execute(query)
        row = datamine.fetchone()
        while row is not None and rows != 0:
            tabbed = tabulate(row, tablefmt='grid')
            print(tabbed)
            rows -= 1
            row = datamine.fetchone()


@main.command()
@click.option('--username', '-u', help='Username for Upsight.com.')
@click.password_option()
@click.option('--query', '-q', help='Query string to execute.')
@click.option('--filename', '-f', help='File to download to.')
def download(username, password, query, filename):
    """Download query results to given filename."""
    with Datamine(username, password) as datamine:
        datamine.execute(query)
        datamine.download(filename)

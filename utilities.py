""" Utilities for WOB ZZ ETL based on pygrametl framework.

Various utilities for:
- data-munging of WOB ZZ dataset into the right format
- MS SQL Server specific methods on top of pygrametl
"""

from decimal import *
import pandas as pd

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


def parse_boolean(value, default=''):
    """Method for parsing boolean 'J'/'N'/leeg into 1,0 or null."""
    if value == None:
        return None
    elif value.upper() == 'J':
        return 1
    elif value.upper() == 'N':
        return 0
    else:
        return None


def parse_codes(value, zfill_length, default='?'):
    """Method for parsing varchar codes.

    Arguments:
    - zfill_length: length to which value should be left-padded with zeros
    - default: value for nulls/blanks
    """
    if value in ['','0']:
        return default
    else:
        try:
            value = value.upper()
            return str(value).zfill(zfill_length)
        except Exception:
            return default


def parse_dates(value, default='10000101'):
    """ Method for parsing dates to ISO format.

    Assumes input data is of format 'YYYYMMDD' with output 'YYYY-MM-DD'
    Blank dates are set to 1000-01-01.
    """
    if value == None:
        value = default
    try:
        value = str(value)
        return '-'.join([value[0:4], value[4:6], value[6:8]])
    except Exception:
        return '-'.join([default[0:4], default[4:6], default[6:8]])


def parse_nulls(string, default='0000'):
    """ Check for nulls or blanks, return default if found."""
    if pd.isnull(string):
        return default
    else:
        return string.zfill(4)


def parse_money(value, default=Decimal(0)):
    """Method for parsing money values from cents to decimals."""
    try:
        return Decimal(value)/Decimal(100)
    except Exception:
        return default


def datetime_to_mssql_string(datetime, default='1000-04-04 00:00:00'):
    """ parse dates in mssql datetime string format with 1000-04-04 for blanks"""
    try:
        return datetime.strftime('%Y-%m-%d') + (' 00:00:00')
    except Exception:
        return default


def get_columns(db, schema, table, cursor):
    """ Get columns in right order from a table."""
    stmt = ('''select column_name, data_type from information_schema.columns
               where table_catalog = '{}'
               and   table_schema = '{}'
               and   table_name = '{}'
            ''')
    cursor.execute(stmt.format(db, schema, table))
    result = pd.DataFrame(cursor.fetchall())
    return result[0].tolist()

def get_column_types(db, schema, table, cursor):
    """ Get columns in right order from a table."""
    stmt = ('''select data_type from information_schema.columns
               where table_catalog = '{}'
               and   table_schema = '{}'
               and   table_name = '{}'
            ''')
    cursor.execute(stmt.format(db, schema, table))
    result = pd.DataFrame(cursor.fetchall())
    return result[0].tolist()





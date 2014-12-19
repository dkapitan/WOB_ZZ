""" Script to load the staged dimension files.

Load staged dimension files into WOB_ZZ datawarehouse. MS SQL assumes columns
are identical and in the right order for bulk insert.

Beware of MS SQL inferno when working from OSX / Python:
- rowterminator = '0x0a' due to differences Windows vs. UNIX
- ms sql want full datetime, e.g. 2007-01-10 00:00:00, for dates
- ms sql can't deal with text delimiters --> do without, check no delimiters in fields
    open statements with " or ''' and use single quotes for strings in sql statements!!

?? autocommit on for pymssql connection as solution to earlier bugs??

Use latin-1 as encoding standard since SQL Server does not support utf-8
"""

import pymssql as sql
import configparser
import os

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

def load_staged_dimension(source_file, target_table, cursor):
    print("Truncating {}:".format(target_table))
    print("    sql> truncate table {}".format(target_table))
    cursor.execute("truncate table {}".format(target_table))
    print("    number of rows affected: {}".format(cursor.rowcount))
    stmt = ('''bulk insert {}
               from '{}'
               with (firstrow=2,
                     fieldterminator=';',
                     rowterminator='0x0a',
                     codepage='1252')''').\
        format(target_table, source_file)
    print("Loading {} ...".format(target_table))
    print("    sql> " + stmt)
    cursor.execute(stmt)
    print("    number of rows affected: {}".format(cursor.rowcount))

def main():
    config = configparser.ConfigParser()
    config.read('/opt/projects/wob_zz/config.ini')
    login = {
        'user': config.get('local_mssql', 'user'),
        'password': config.get('local_mssql', 'password'),
        'server': config.get('local_mssql', 'server'),
        'port': config.get('local_mssql', 'port'),
        'database': config.get('wob_zz', 'database')}

    cnx = sql.connect(**login)

    # turn autocommit on
    cnx.autocommit(True)
    cursor = cnx.cursor()

    # get all staging file names in staging_path
    staging_path = config.get('wob_zz', 'staging_path')
    csv_files = [fn for fn in os.listdir(staging_path)
                 if any([fn.endswith(ext) for ext in ['csv']])]

    # format staging path for Windows OS with escaped backslashes
    # \\psf is mountpoint for OSX
    windows_path = '\\\\psf' + staging_path.replace('/','\\') + '\\'
    for file in csv_files:
        table = '.'.join(file.split('.')[0:2])
        windows_source = windows_path + file
        load_staged_dimension(windows_source, table, cursor)

if __name__ == '__main__':
    main()
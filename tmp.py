import configparser
import pandas as pd
import pymssql as sql
import csv
import os

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

''' old versions DIM.SUBTRAJECT and FCT.SUBTRAJECT without bulkloading
DIM_SUBTRAJECTNUMMER = CachedDimension(
    name='DIM.SUBTRAJECTNUMMER',
    key='stn_id',
    attributes=['stn_subtraject_id'],
    size=0,
    prefill=True)



FCT_SUBTRAJECT = FactTable(
    name='FCT.SUBTRAJECT_2012',
    keyrefs=['dag_id_begindatum_zorgtraject', 'dag_id_einddatum_zorgtraject', 'dag_id_begindatum_subtraject',
             'dag_id_einddatum_subtraject', 'dag_id_declaratiedatum',
             'dia_id', 'stn_id', 'zgt_id', 'zgv_id', 'zpr_id'],
    measures=['is_hoofdtraject', 'is_aanspraak_zvw', 'is_aanspraak_zvw_toegepast', 'heeft_zorgactiviteit_met_machtiging',
              'heeft_oranje_zorgactiviteit', 'is_zorgactiviteitvertaling_toegepast', 'fct_omzet_ziekenhuis', 'fct_omzet_honorarium_totaal']
)
'''

config = configparser.ConfigParser()
config.read('/opt/projects/wob_zz/config.ini')

login = {
    'user': config.get('local_mssql', 'user'),
    'password': config.get('local_mssql', 'password'),
    'server': config.get('local_mssql', 'server'),
    'port': config.get('local_mssql', 'port'),
    'database': config.get('local_mssql', 'database')
    }

# log TDS for bug tracing
# os.environ['TDSDUMP'] = 'stdout'

cnx = sql.connect(**login)
cursor = cnx.cursor()

def get_columns(db, schema, table, cursor):
    stmt = ('''select column_name from information_schema.columns
             where table_catalog = '{}'
             and   table_schema = '{}'
             and   table_name = '{}'
            ''')
    cursor.execute(stmt.format(db, schema, table))
    result = pd.DataFrame(cursor.fetchall())
    return result[0].tolist()

print(get_columns('wob_zz', 'DIM', 'DAG', cursor))

# workaround bug-fix: need to query using _cnx with _mssql, after that pymssql works ok
cursor = cnx.cursor()
cursor.execute('select 1')
print('initialize pymssql cursor:\t' + str(cursor.description))
_cnx.execute_query('select 1')
for row in _cnx: print('initialize _mssql query:\t' + str(row))

cursor.execute('select 1')
print('check pymssql cursor returns (1,):\t' + str(cursor.fetchall()[0]))

connection = etl.ConnectionWrapper(cnx)
connection.setasdefault()


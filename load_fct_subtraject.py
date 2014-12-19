#!/usr/bin/env python
""" Script to load fact-subtraject from wob_zz

ETL of WOB ZZ dataset based on pygrametl framework.

NB:     python table names are DIM_xxx, FCT_yyy;
        MS SQL schema.table names are DIM.xxx, FCT.yyy

NNB:    tempdest is stored in /var/folders/ ...
        This path should be available to MS SQL Server,
        e.g. via sharing in Parallels


"""

import bz2
import csv
import configparser
import pymssql as sql
import time
import pygrametl as etl
from pygrametl.tables import CachedDimension, BulkFactTable, BulkDimension
from wob_zz import *

# import cProfile, pstats, StringIO

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


def mssql_bulkloader(tablename, attributes, fieldsep, rowsep, nullsubst, tempdest):
    """Bulkloader using MS SQL Server bulk insert.

    This works for the following setup:
    - Python 3 (anaconda) running on OSX
    - SQL Server 2014 running on Windows 7 Ultimate running on Parallels 9
    - pymssql / FreeTDS as python DB API
    - /var/library directory that has tempdest files is shared via Parallels

    NB:
    - rowterminator = '0x0a' due to differences Windows vs. UNIX
    - ms sql want full datetime, e.g. 2007-01-10 00:00:00, for dates
    - ms sql can't deal with text delimiters --> do without,
        check no delimiters in fields
    - open statements with " or ''' and use single quotes
        for strings in sql statements!!
    """
    global cur
    win_temp = '\\\\psf' + tempdest.replace('/','\\')
    stmt = ('''bulk insert {} from '{}'
               with (firstrow=1,
               fieldterminator='\\t',
               rowterminator='0x0a',
               codepage='1252')
             '''.format(tablename, win_temp))
    print("    sql> " + stmt)
    cur.execute(stmt)
    print("    number of rows affected: {}".format(cur.rowcount))


# setup connection to database
config = configparser.ConfigParser()
config.read('/opt/projects/wob_zz/config.ini')

login = {
    'user': config.get('local_mssql', 'user'),
    'password': config.get('local_mssql', 'password'),
    'server': config.get('local_mssql', 'server'),
    'port': config.get('local_mssql', 'port'),
    'database': config.get('wob_zz', 'database')
    }

cnx = sql.connect(**login)
cur = cnx.cursor()
connection = etl.ConnectionWrapper(cnx)
connection.setasdefault()


# define dimension object for ETL
# Note that:
# - pygrametl object table names are DIM_xxx, FCT_yyy
# - MS SQL schema.table names are DIM.xxx, FCT.yyy
DIM_AFSLUITREDEN = CachedDimension(
name='DIM.AFSLUITREDEN',
key='afs_id',
attributes=['afs_afsluitreden_code'],
size=0,
prefill=True
)

DIM_BEHANDELING = CachedDimension(
    name='DIM.BEHANDELING',
    key='beh_id',
    attributes=['beh_dbc_specialisme_code', 'beh_dbc_behandeling_code'],
    size=0,
    prefill=True
)

DIM_DAG = CachedDimension(
    name='DIM.DAG',
    key='dag_id',
    attributes=['dag_datum'],
    size=0,
    prefill=True
)

DIM_DECLARATIE = CachedDimension(
    name='DIM.DECLARATIE',
    key='dcl_id',
    attributes=['dcl_dbc_declaratie_code'],
    size=0,
    prefill=True
)

DIM_DIAGNOSE = CachedDimension(
    name='DIM.DIAGNOSE',
    key='dia_id',
    attributes=['dia_dbc_specialisme_code', 'dia_dbc_diagnose_code'],
    size=0,
    prefill=True
)

DIM_LAND = CachedDimension(
    name='DIM.LAND',
    key='lnd_id',
    attributes=['lnd_iso_land_code'],
    size=0,
    prefill=True
)

DIM_SUBTRAJECTNUMMER = BulkDimension(
    name='DIM.SUBTRAJECTNUMMER',
    key='stn_id',
    attributes=['stn_subtraject_id', 'stn_subtrajectnummer',
                'stn_zorgtrajectnummer', 'stn_zorgtrajectnummer_parent'],
    lookupatts=['stn_subtraject_id'],
    nullsubst='',
    fieldsep='\t',
    rowsep='\r\n',
    usefilename=True,
    bulkloader=mssql_bulkloader
)

DIM_ZORGPRODUCT = CachedDimension(
    name='DIM.ZORGPRODUCT',
    key='zpr_id',
    attributes=['zpr_dbc_zorgproduct_code'],
    size=0,
    prefill=True
)

DIM_ZORGTYPE = CachedDimension(
    name='DIM.ZORGTYPE',
    key='zgt_id',
    attributes=['zgt_dbc_specialisme_code', 'zgt_dbc_zorgtype_code'],
    size=0,
    prefill=True
)

DIM_ZORGVERLENERSOORT = CachedDimension(
    name='DIM.ZORGVERLENERSOORT',
    key='zvs_id',
    attributes=['zvs_vektis_zorgverlenersoort_code'],
    size=0,
    prefill=True
)

DIM_ZORGVRAAG = CachedDimension(
    name='DIM.ZORGVRAAG',
    key='zgv_id',
    attributes=['zgv_dbc_specialisme_code', 'zgv_dbc_zorgvraag_code'],
    size=0,
    prefill=True
)

FCT_SUBTRAJECT = BulkFactTable(
    name='FCT.SUBTRAJECT',
    keyrefs=['beh_id', 'dag_id_begindatum_zorgtraject',
             'dag_id_einddatum_zorgtraject', 'dag_id_begindatum_subtraject',
             'dag_id_einddatum_subtraject', 'dag_id_declaratiedatum',
             'dia_id', 'stn_id', 'zgt_id', 'zgv_id', 'zpr_id',
             'zvs_id_behandelend', 'zvs_id_verwijzend'],
    measures=['geslacht', 'heeft_oranje_zorgactiviteit',
              'heeft_zorgactiviteit_met_machtiging',
              'is_hoofdtraject', 'is_aanspraak_zvw',
              'is_aanspraak_zvw_toegepast',
              'is_zorgactiviteitvertaling_toegepast', 'fct_omzet_ziekenhuis',
              'fct_omzet_honorarium_totaal'],
    nullsubst='',
    fieldsep='\t',
    rowsep='\r\n',
    usefilename=True,
    bulkloader=mssql_bulkloader
)

def load_str_dot(file, config):
    """Method for loading one subtraject file of WOB ZZ DOT

    Main ETL method for WOB ZZ subtrajecten.
    Requires active pygrametl connection as global.
    """
    global connection
    paths = {'data_path': config.get('wob_zz', 'data_path'),
             'staging_path': config.get('wob_zz', 'staging_path')}
    names_STR = ['datum_aanmaak', 'landcode', 'geslacht',
                 'verwijzend_specialisme', 'zorgtrajectnummer',
                 'zorgtrajectnummer_parent', 'begindatum_zorgtraject',
                 'einddatum_zorgtraject', 'declaratiedatasetnummer',
                 'subtrajectnummer', 'subtraject_id', 'declaratiecode',
                 'behandelend_specialisme', 'zorgtypecode', 'zorgvraagcode',
                 'typerende_diagnose', 'icd10_vertaling_diagnose',
                 'hoofdtraject_indicatie', 'zorgproductcode',
                 'dbc_reden_sluiten', 'aanspraak_zvw',
                 'aanspraak_zvw_toegepast', 'zorgact_met_machtiging',
                 'oranje_zorgactiviteit', 'zorgactiviteitvertaling_toegepast',
                 'begindatum_subtraject', 'einddatum_subtraject',
                 'declaratiedatum', 'dbc_ziekenhuiskosten',
                 'honorarium_totaal']

    source_file = bz2.open(paths['data_path'] + '/' + file, mode='rt')
    source = csv.DictReader(source_file, delimiter=';',
                            quotechar='"', fieldnames=names_STR)

    starttime = time.localtime()
    start_s = time.time()
    print('{} - Start processing file: {}'.
          format(time.strftime('%H:%M:%S', starttime), file))

    name_mapping = {
        'afl_afsluitreden_code'         : 'dbc_reden_sluiten',
        #'beh_dbc_specialisme_code'      : 'behandelend_specialisme',
        #'beh_dbc_behandeling_code'      : 'behandelcode',
        'dcl_declaratie_code'           : 'declaratiecode',
        'dia_dbc_specialisme_code'      : 'behandelend_specialisme',
        'dia_dbc_diagnose_code'         : 'typerende_diagnose',
        'geslacht'                      : 'geslacht',
        'heeft_zorgactiviteit_met_machtiging': 'zorgact_met_machtiging',
        'heeft_oranje_zorgactiviteit'   : 'oranje_zorgactiviteit',
        'is_aanspraak_zvw'              : 'aanspraak_zvw',
        'is_aanspraak_zvw_toegepast'    : 'aanspraak_zvw_toegepast',
        'is_hoofdtraject'               : 'hoofdtraject_indicatie',
        'is_zorgactiviteitvertaling_toegepast': 'zorgactiviteitvertaling_toegepast',
        'lnd_land_code'                 : 'landcode',
        'stn_subtraject_id'             : 'subtraject_id',
        'stn_subtrajectnummer'          : 'subtrajectnummer',
        'stn_zorgtrajectnummer'         : 'zorgtrajectnummer',
        'stn_zorgtrajectnummer_parent'  : 'zorgtrajectnummer_parent',
        'zgt_dbc_specialisme_code'      : 'behandelend_specialisme',
        'zgt_dbc_zorgtype_code'         : 'zorgtypecode',
        'zgv_dbc_specialisme_code'      : 'behandelend_specialisme',
        'zgv_dbc_zorgvraag_code'        : 'zorgvraagcode',
        'zpr_dbc_zorgproduct_code'      : 'zorgproductcode',
        'fct_omzet_ziekenhuis'          : 'dbc_ziekenhuiskosten',
        'fct_omzet_honorarium_totaal'   : 'honorarium_totaal',

        }

    for row in source:

        # convert datecolumns to appropriate format
        row['begindatum_zorgtraject'] = parse_dates(row['begindatum_zorgtraject'])
        row['einddatum_zorgtraject'] = parse_dates(row['einddatum_zorgtraject'])
        row['begindatum_subtraject'] = parse_dates(row['begindatum_subtraject'])
        row['einddatum_subtraject'] = parse_dates(row['einddatum_subtraject'])
        row['declaratiedatum'] = parse_dates(row['declaratiedatum'])

        # ensure DBC codes are filled to right length
        row['verwijzend_specialisme'] = \
            parse_codes(row['verwijzend_specialisme'], 4, '_?_')
        row['behandelend_specialisme'] = \
            parse_codes(row['behandelend_specialisme'], 4, '_?_')
        row['zorgtypecode'] = parse_codes(row['zorgtypecode'], 2, '??')
        row['zorgvraagcode'] = parse_codes(row['zorgvraagcode'], 4, '_?_')
        row['typerende_diagnose'] = parse_codes(row['typerende_diagnose'], 4, '_?_')
        row['zorgproductcode'] = parse_codes(row['zorgproductcode'], 9, '_?_')

        # convert geslacht into int conform COD046_NEN / Vektis
        row['geslacht'] = etl.getint(row['geslacht'], default=0)

        # convert booleans
        row['hoofdtraject_indicatie'] = parse_boolean(row['hoofdtraject_indicatie'])
        row['aanspraak_zvw'] = parse_boolean(row['aanspraak_zvw'])
        row['aanspraak_zvw_toegepast'] = parse_boolean(row['aanspraak_zvw_toegepast'])
        row['oranje_zorgactiviteit'] = parse_boolean(row['oranje_zorgactiviteit'])
        row['zorgact_met_machtiging'] = parse_boolean(row['zorgact_met_machtiging'])
        row['zorgactiviteitvertaling_toegepast'] = parse_boolean(row['zorgactiviteitvertaling_toegepast'])

        # convert money values into decimals
        row['dbc_ziekenhuiskosten'] = parse_money(row['dbc_ziekenhuiskosten'])
        row['honorarium_totaal'] = parse_money(row['honorarium_totaal'])

        # derive dimension_ids
        row['beh_id'] = -1 # no behandelcodes in DOT per 2012-01-01
        row['dag_id_begindatum_zorgtraject'] = DIM_DAG.lookup(row, {'dag_datum': 'begindatum_zorgtraject'})
        row['dag_id_einddatum_zorgtraject'] = DIM_DAG.lookup(row, {'dag_datum': 'einddatum_zorgtraject'})
        row['dag_id_begindatum_subtraject'] = DIM_DAG.lookup(row, {'dag_datum': 'begindatum_subtraject'})
        row['dag_id_einddatum_subtraject'] = DIM_DAG.lookup(row, {'dag_datum': 'einddatum_subtraject'})
        row['dag_id_declaratiedatum'] = DIM_DAG.lookup(row, {'dag_datum': 'declaratiedatum'})
        row['dia_id'] = DIM_DIAGNOSE.ensure(row, name_mapping)
        row['stn_id'] = DIM_SUBTRAJECTNUMMER.ensure(row, name_mapping)
        row['zgt_id'] = DIM_ZORGTYPE.ensure(row, name_mapping)
        row['zgv_id'] = DIM_ZORGVRAAG.ensure(row, name_mapping)
        row['zpr_id'] = DIM_ZORGPRODUCT.ensure(row, name_mapping)
        row['zvs_id_behandelend'] = \
            DIM_ZORGVERLENERSOORT.ensure(row, {'zvs_vektis_zorgverlenersoort_code': 'behandelend_specialisme'})
        row['zvs_id_verwijzend'] = \
            DIM_ZORGVERLENERSOORT.ensure(row, {'zvs_vektis_zorgverlenersoort_code': 'verwijzend_specialisme'})

        # insert fact table
        FCT_SUBTRAJECT.insert(row, name_mapping)

    connection.commit()

    end_s = time.time()
    endtime = time.localtime()
    print('{} - Finished processing {}'.
          format(time.strftime('%H:%M:%S', endtime), file))
    print('           Processing time: %0.2f seconds ' % (end_s - start_s))


def main():
    """ Main routine for loading WOB ZZ subtrajecten."""
    global cnx, cur

    # trunctate FCT.SUBTRAJECT
    cur.execute("truncate table FCT.SUBTRAJECT")
    cnx.commit()

    # loop to load per file: DOT
    files = []
    for year in range(2012, 2015, 1):
        for month in range(1, 13, 1):
            files.append('{}/DIS_RAP_SZG_WOB_STR_700_{}_20140410_1.csv.bz2'
                        .format(year, (str(year)+str(month).zfill(2))))

    for file in files:
            load_str_dot(file, config)

    cnx.close()


if __name__ == "__main__":
    main()
""" Prepares Electronische typeringslijst for ETL in WOB_ZZ DWH.

Reference tables from DBC Onderhoud - totaalbestand contain different
version of the same code. For the datawarehouse, the latest c.q. most
recent version is kept and all previous versions are ignored.

Following dimension tables are generated:

    DIM_BEHANDLING - behandeling codes of old DBC regime up to 2011-12-31

    DIM_DIAGNOSE - diagnose codes for old DBC and new DOT system. Enriched
    with zorgproductgroep (beslisboom) mapping when available.
    Current version does not include mapping to ICD10 or DRGs,
    which is possible with DHD diagnose thesaurus. This may be added in future.
    Note that dbc_dia and IDC-10 is a many-to-many relationship,
    so not strict hierarchical.

    DIM_ZORGTYPE

    DIM_ZORGVRAAG
"""

import configparser
import pymssql as sql
import pandas as pd
from wob_zz import *

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


def main():

    # setup database connection
    config = configparser.ConfigParser()
    config.read('/opt/projects/wob_zz/config.ini')
    login = {
        'user': config.get('local_mssql', 'user'),
        'password': config.get('local_mssql', 'password'),
        'server': config.get('local_mssql', 'server'),
        'port': config.get('local_mssql', 'port'),
        'database': config.get('wob_zz', 'database')}
    cnx = sql.connect(**login)
    cursor = cnx.cursor()

    # configure files
    paths = {'data_path': config.get('wob_zz', 'dbco_path'),
             'staging_path': config.get('wob_zz', 'staging_path')}
    data_file = paths['data_path'] \
        + '/20140601 Totaalbestand uitlevering v20140501' \
        + '/20140101 Elektronische Typeringslijst v20131114.csv'

    # read DBC typeringslijst file into dataframe and reorder
    data = pd.read_csv(data_file, sep=';',
        dtype=str, encoding='latin1',
        parse_dates=['Ingangsdatum','Afloopdatum'],
        usecols=['Specialisme code AGB', 'As omschrijving', 'Component Code',
            'Component omschrijving lang', 'Hoofdgroep code',
            'Hoofdgroep omschrijving lang', 'Subgroep code',
            'Subgroep omschrijving lang', 'Ingangsdatum',
            'Afloopdatum'])


    ###########################
    # process behandeling codes
    ###########################
    df = data
    df = df.ix[df['As omschrijving'] == 'behandeling']
    df = df.drop(['As omschrijving'], axis=1)

    # map names to our own naming convention
    mapping = {'Specialisme code AGB':          'beh_dbc_specialisme_code',
               'Component Code':                'beh_dbc_behandeling_code',
               'Component omschrijving lang':   'beh_dbc_behandeling_omschrijving',
               'Hoofdgroep code':               'beh_dbc_hoofdgroep_code',
               'Hoofdgroep omschrijving lang':  'beh_dbc_hoofdgroep_omschrijving',
               'Subgroep code':                 'beh_dbc_subgroep_code',
               'Subgroep omschrijving lang':    'beh_dbc_subgroep_omschrijving',
               'Ingangsdatum':                  'beh_dbc_begindatum',
               'Afloopdatum':                   'beh_dbc_einddatum'}
    df.rename(columns=mapping, inplace=True)

    # format fields
    df['beh_dbc_specialisme_code'] = df['beh_dbc_specialisme_code'].apply(lambda x: str(x).zfill(4))
    df['beh_dbc_behandeling_code'] = df['beh_dbc_behandeling_code'].apply(lambda x: str(x).zfill(4))
    df['beh_dbc_hoofdgroep_code'] = df['beh_dbc_hoofdgroep_code'].apply(lambda x: parse_nulls(x))
    df['beh_dbc_begindatum'] = df['beh_dbc_begindatum'].apply(lambda x: datetime_to_mssql_string(x))
    df['beh_dbc_einddatum'] = df['beh_dbc_einddatum'].apply(lambda x: datetime_to_mssql_string(x))

    # drop duplicates, take latest c.q. most current verion
    df.drop_duplicates(cols=['beh_dbc_specialisme_code',
                             'beh_dbc_behandeling_code'],
                       take_last=True,
                       inplace=True)
    # add id and reorder
    df['beh_id'] = range(1,len(df)+1,1)
    target_columns = get_columns(login['database'], 'DIM', 'BEHANDELING', cursor)
    for column in target_columns:
        if column not in df.columns:
            df[column] = ''
    unknown = [{'beh_id': -1,
               'beh_dbc_specialisme_code': '_?_',
               'beh_dbc_behandeling_code':  '_?_',
               'beh_dbc_behandeling_omschrijving': '_?_',
               'beh_dbc_hoofdgroep_code': '_?_',
               'beh_dbc_hoofdgroep_omschrijving': '_?_',
               'beh_dbc_subgroep_code': '_?_',
               'beh_dbc_subgroep_omschrijving': '_?_',
               'beh_dbc_begindatum': '1000-01-01 00:00:00',
               'beh_dbc_einddatum': '1000-01-01 00:00:00'}]
    df = df.append(pd.DataFrame(unknown), ignore_index=True)
    df = df[target_columns]
    df = df.sort(columns='beh_id')

    # write output to .csv
    df.to_csv(paths['staging_path'] + '/DIM.BEHANDELING.csv', sep=';',
              header=True, index = False, encoding='cp1252',
              quoting=None, na_rep='_?_')



    ########################
    # process diagnose codes
    ########################
    df = data
    df = df.ix[df['As omschrijving'] == 'diagnose']
    df = df.drop(['As omschrijving'], axis=1)

    # map names to our own naming convention
    mapping = {'Specialisme code AGB':          'dia_dbc_specialisme_code',
               'Component Code':                'dia_dbc_diagnose_code',
               'Component omschrijving lang':   'dia_dbc_diagnose_omschrijving',
               'Hoofdgroep code':               'dia_dbc_hoofdgroep_code',
               'Hoofdgroep omschrijving lang':  'dia_dbc_hoofdgroep_omschrijving',
               'Subgroep code':                 'dia_dbc_subgroep_code',
               'Subgroep omschrijving lang':    'dia_dbc_subgroep_omschrijving',
               'Ingangsdatum':                  'dia_dbc_begindatum',
               'Afloopdatum':                   'dia_dbc_einddatum'}
    df.rename(columns=mapping, inplace=True)

    # format fields
    df['dia_dbc_specialisme_code'] = df['dia_dbc_specialisme_code'].apply(lambda x: str(x).zfill(4))
    df['dia_dbc_diagnose_code'] = df['dia_dbc_diagnose_code'].apply(lambda x: str(x).zfill(4))
    df['dia_dbc_hoofdgroep_code'] = df['dia_dbc_hoofdgroep_code'].apply(lambda x: parse_nulls(x))
    df['dia_dbc_subgroep_code'] = df['dia_dbc_subgroep_code'].apply(lambda x: parse_nulls(x))
    df['dia_dbc_begindatum'] = df['dia_dbc_begindatum'].apply(lambda x: datetime_to_mssql_string(x))
    df['dia_dbc_einddatum'] = df['dia_dbc_einddatum'].apply(lambda x: datetime_to_mssql_string(x))

    # drop duplicates, take latest c.q. most current verion
    df.drop_duplicates(cols=['dia_dbc_specialisme_code',
                             'dia_dbc_diagnose_code'],
                       take_last=True, inplace=True)

    # enrich diagnose codes with most recent zorgproductgroep ('beslisbomen') if available
    zpg_file = paths['data_path'] \
        + '/20140601 Totaalbestand uitlevering v20140501' \
        + '/20140101 Relatie Diagnose Zorgproductgroepen Tabel v20130926.csv'
    zpg = pd.read_csv(zpg_file, sep=";", dtype=str, encoding='latin1',
                          parse_dates=['Ingangsdatum','Einddatum'],
                          usecols=['Specialisme code AGB', 'Diagnose code',
                                   'Zorgproductgroep code', 'Ingangsdatum',
                                   'Einddatum'])
    zpg.rename(columns={'Specialisme code AGB': 'dia_dbc_specialisme_code',
                        'Diagnose code': 'dia_dbc_diagnose_code',
                        'Zorgproductgroep code': 'dia_dbc_zorgproductgroep_code'},
               inplace=True)
    zpg['dia_dbc_diagnose_code'] = zpg['dia_dbc_diagnose_code'].apply(lambda x: str(x).zfill(4))
    zpg.drop_duplicates(cols=['dia_dbc_specialisme_code', 'dia_dbc_diagnose_code'],
                        take_last=True, inplace=True)
    zpg = zpg.drop(['Ingangsdatum', 'Einddatum'], axis=1)

    zpgo_file = paths['data_path'] \
        + '/20140601 Totaalbestand uitlevering v20140501' \
        + '/20140101 Zorgproductgroepen Tabel v20131114.csv'
    zpgo = pd.read_csv(zpgo_file, sep=';', dtype=str, encoding='latin1',
                       usecols=['Zorgproductgroep code',
                                'Zorgproductgroep omschrijving',
                                'Ingangsdatum',
                                'Einddatum'])
    zpgo.rename(columns={'Zorgproductgroep code': 'dia_dbc_zorgproductgroep_code',
                         'Zorgproductgroep omschrijving': 'dia_dbc_zorgproductgroep_omschrijving'},
                inplace=True)
    zpgo.drop_duplicates(cols=['dia_dbc_zorgproductgroep_code'],
                         take_last=True, inplace=True)
    zpgo = zpgo.drop(['Ingangsdatum', 'Einddatum'], axis=1)

    df = pd.merge(df, zpg)
    df = pd.merge(df, zpgo)

    # add id and reorder
    df['dia_id'] = range(1,len(df)+1,1)
    target_columns = get_columns(login['database'], 'DIM', 'DIAGNOSE', cursor)
    for column in target_columns:
        if column not in df.columns:
            df[column] = ''
    unknown = [{'dia_id': -1,
           'dia_dbc_specialisme_code': '_?_',
           'dia_dbc_diagnose_code':  '_?_',
           'dia_dbc_diagnose_omschrijving': '_?_',
           'dia_dbc_hoofdgroep_code': '_?_',
           'dia_dbc_hoofdgroep_omschrijving': '_?_',
           'dia_dbc_subgroep_code': '_?_',
           'dia_dbc_subgroep_omschrijving': '_?_',
           'dia_dbc_begindatum': '1000-01-01 00:00:00',
           'dia_dbc_einddatum': '1000-01-01 00:00:00',
           'dia_dbc_zorgproductgroep_code': '_?_',
           'dia_dbc_zorgproductgroep_omschrijving': '_?_'}]
    df = df.append(pd.DataFrame(unknown), ignore_index=True)
    df = df[target_columns]
    df = df.sort(columns='dia_id')

    # write output to .csv
    df.to_csv(paths['staging_path'] + '/DIM.DIAGNOSE.csv', sep=';',
              header=True, index=False, encoding='cp1252',
              quoting=None, na_rep='_?_')


    #########################
    # process zorgtype codes
    #########################
    df = data
    df = df.ix[df['As omschrijving'] == 'zorgtype']
    df = df.drop(['As omschrijving'], axis=1)

    # map names to our own naming convention
    mapping = {'Specialisme code AGB':          'zgt_dbc_specialisme_code',
               'Component Code':                'zgt_dbc_zorgtype_code',
               'Component omschrijving lang':   'zgt_dbc_zorgtype_omschrijving',
               'Hoofdgroep code':               'zgt_dbc_hoofdgroep_code',
               'Hoofdgroep omschrijving lang':  'zgt_dbc_hoofdgroep_omschrijving',
               'Subgroep code':                 'zgt_dbc_subgroep_code',
               'Subgroep omschrijving lang':    'zgt_dbc_subgroep_omschrijving',
               'Ingangsdatum':                  'zgt_dbc_begindatum',
               'Afloopdatum':                   'zgt_dbc_einddatum'}
    df.rename(columns=mapping, inplace=True)

    # format fields
    df['zgt_dbc_specialisme_code'] = df['zgt_dbc_specialisme_code'].apply(lambda x: str(x).zfill(4))
    df['zgt_dbc_zorgtype_code'] = df['zgt_dbc_zorgtype_code'].apply(lambda x: str(x).zfill(2))
    df['zgt_dbc_hoofdgroep_code'] = df['zgt_dbc_hoofdgroep_code'].apply(lambda x: str(x).zfill(4))
    df['zgt_dbc_begindatum'] = df['zgt_dbc_begindatum'].apply(lambda x: datetime_to_mssql_string(x))
    df['zgt_dbc_einddatum'] = df['zgt_dbc_einddatum'].apply(lambda x: datetime_to_mssql_string(x))

    # drop duplicates, take latest c.q. most current verion
    df.drop_duplicates(cols=['zgt_dbc_specialisme_code',
                             'zgt_dbc_zorgtype_code'],
                       take_last=True, inplace=True)

    # add id and reorder
    df['zgt_id'] = range(1,len(df)+1,1)
    target_columns = get_columns(login['database'], 'DIM', 'ZORGTYPE', cursor)
    for column in target_columns:
        if column not in df.columns:
            df[column] = ''
    unknown = [{'zgt_id': -1,
       'zgt_dbc_specialisme_code': '_?_',
       'zgt_dbc_zorgtype_code':  '??',
       'zgt_dbc_zorgtype_omschrijving': '_?_',
       'zgt_dbc_hoofdgroep_code': '_?_',
       'zgt_dbc_hoofdgroep_omschrijving': '_?_',
       'zgt_dbc_subgroep_code': '_?_',
       'zgt_dbc_subgroep_omschrijving': '_?_',
       'zgt_dbc_begindatum': '1000-01-01 00:00:00',
       'zgt_dbc_einddatum': '1000-01-01 00:00:00'}]
    df = df.append(pd.DataFrame(unknown), ignore_index=True)
    df = df[target_columns]
    df = df.sort(columns='zgt_id')

    # write output to .csv
    df.to_csv(paths['staging_path'] + '/DIM.ZORGTYPE.csv', sep=';',
              header=True, index=False, encoding='cp1252',
              quoting=None, na_rep= '_?_')


    #########################
    # process zorgvraag codes
    #########################
    df = data
    df = df.ix[df['As omschrijving'] == 'zorgvraag']
    df = df.drop(['As omschrijving'], axis=1)

    # map names to our own naming convention
    mapping = {'Specialisme code AGB':          'zgv_dbc_specialisme_code',
               'Component Code':                'zgv_dbc_zorgvraag_code',
               'Component omschrijving lang':   'zgv_dbc_zorgvraag_omschrijving',
               'Hoofdgroep code':               'zgv_dbc_hoofdgroep_code',
               'Hoofdgroep omschrijving lang':  'zgv_dbc_hoofdgroep_omschrijving',
               'Subgroep code':                 'zgv_dbc_subgroep_code',
               'Subgroep omschrijving lang':    'zgv_dbc_subgroep_omschrijving',
               'Ingangsdatum':                  'zgv_dbc_begindatum',
               'Afloopdatum':                   'zgv_dbc_einddatum'}
    df.rename(columns=mapping, inplace=True)

    # format fields
    df['zgv_dbc_specialisme_code'] = df['zgv_dbc_specialisme_code'].apply(lambda x: str(x).zfill(4))
    df['zgv_dbc_zorgvraag_code'] = df['zgv_dbc_zorgvraag_code'].apply(lambda x: str(x).zfill(4))
    df['zgv_dbc_hoofdgroep_code'] = df['zgv_dbc_hoofdgroep_code'].apply(lambda x: str(x).zfill(4))
    df['zgv_dbc_begindatum'] = df['zgv_dbc_begindatum'].apply(lambda x: datetime_to_mssql_string(x))
    df['zgv_dbc_einddatum'] = df['zgv_dbc_einddatum'].apply(lambda x: datetime_to_mssql_string(x))

    # drop duplicates, take latest c.q. most current verion
    df.drop_duplicates(cols=['zgv_dbc_specialisme_code', 'zgv_dbc_zorgvraag_code'], take_last=True, inplace=True)

    # add id and reorder
    df['zgv_id'] = range(1,len(df)+1,1)
    target_columns = get_columns(login['database'], 'DIM', 'ZORGVRAAG', cursor)
    for column in target_columns:
        if column not in df.columns:
            df[column] = ''
    unknown = [{'zgv_id': -1,
       'zgv_dbc_specialisme_code': '_?_',
       'zgv_dbc_zorgvraag_code':  '??',
       'zgv_dbc_zorgvraag_omschrijving': '_?_',
       'zgv_dbc_hoofdgroep_code': '_?_',
       'zgv_dbc_hoofdgroep_omschrijving': '_?_',
       'zgv_dbc_subgroep_code': '_?_',
       'zgv_dbc_subgroep_omschrijving': '_?_',
       'zgv_dbc_begindatum': '1000-01-01 00:00:00',
       'zgv_dbc_einddatum': '1000-01-01 00:00:00'}]
    df = df.append(pd.DataFrame(unknown), ignore_index=True)
    df = df[target_columns]
    df = df.sort(columns='zgv_id')

    # write output to .csv
    df.to_csv(paths['staging_path'] + '/DIM.ZORGVRAAG.csv', sep = ';',
              header = True, index = False, encoding = 'cp1252',
              quoting=None, na_rep= '_?_')


if __name__ == '__main__':
    main()

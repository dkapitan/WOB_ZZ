""" Prepares DBC Zorgproducten and Zorgproductegroepen reference tables
for ETL in WOB_ZZ DWH.

Reference tables from DBC Onderhoud - totaalbestand contain different
version of the same code. For the datawarehouse, the latest c.q. most
recent version is kept and all previous versions are ignored.

Following dimension tables are generated:

    DIM_ZORGPRODUCT
"""

import configparser
import pymssql as sql
import pandas as pd
from wob_zz import datetime_to_mssql_string, get_columns

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
        + '/20140601 Zorgproducten Tabel v20140501.csv'
    data = pd.read_csv(data_file, sep=';',dtype=str, encoding='latin1',
        parse_dates=['Ingangsdatum','Einddatum'])

    mapping = {
        'Zorgproductcode': 'zpr_dbc_zorgproduct_code',
        'Zorgproductomschrijving': 'zpr_dbc_zorgproduct_omschrijving_lang',
        'Zorgproduct latijnse omschrijving': 'zpr_dbc_zorgproduct_omschrijving_latijn',
        'Zorgproduct consumentenomschrijving': 'zpr_dbc_zorgproduct_omschrijving_consument',
        'Declaratiecode verzekerde zorg': 'zpr_dbc_declaratiecode_verzekerd',
        'Declaratiecode onverzekerde zorg': 'zpr_dbc_declaratiecode_onverzekerd',
        'Zorgproduct WBMV code': 'zpr_dbc_WBMV_code',
        'Zorgproductgroep Code': 'zpr_dbc_zorgproductgroep_code',
        'Ingangsdatum': 'zpr_dbc_begindatum',
        'Einddatum': 'zpr_dbc_einddatum'
    }

    data.rename(columns=mapping, inplace=True)
    data = data.drop(['zpr_dbc_declaratiecode_verzekerd',
                      'zpr_dbc_declaratiecode_onverzekerd',
                      'Mutatie'], axis=1)
    data['zpr_dbc_begindatum'] = data['zpr_dbc_begindatum'].apply(lambda x: datetime_to_mssql_string(x))
    data['zpr_dbc_einddatum'] = data['zpr_dbc_einddatum'].apply(lambda x: datetime_to_mssql_string(x))
    data.drop_duplicates(cols=['zpr_dbc_zorgproduct_code'],
                         take_last=True, inplace=True)

    # enrich zorgproduct codes with zorgproductgroep omschrijving
    zpgo_file = paths['data_path'] \
        + '/20140601 Totaalbestand uitlevering v20140501' \
        + '/20140101 Zorgproductgroepen Tabel v20131114.csv'
    zpgo = pd.read_csv(zpgo_file, sep=';', dtype=str, encoding='latin1',
                       usecols=['Zorgproductgroep code',
                                'Zorgproductgroep omschrijving',
                                'Ingangsdatum',
                                'Einddatum'])
    zpgo.rename(columns={'Zorgproductgroep code': 'zpr_dbc_zorgproductgroep_code',
                         'Zorgproductgroep omschrijving': 'zpr_dbc_zorgproductgroep_omschrijving'},
                inplace=True)
    zpgo.drop_duplicates(cols=['zpr_dbc_zorgproductgroep_code'],
                         take_last=True, inplace=True)
    zpgo = zpgo.drop(['Ingangsdatum', 'Einddatum'], axis=1)

    # enrich WBMV codes with description
    wbmv_file = paths['data_path'] \
        + '/20140601 Totaalbestand uitlevering v20140501' \
        + '/20140101 WBMV Code Tabel v20131114.csv'
    wbmv = pd.read_csv(wbmv_file, sep=';', dtype=str, encoding='latin1')
    wbmv.rename(columns={'WBMV_code': 'zpr_dbc_WBMV_code',
                         'WBMV_code_omschrijving': 'zpr_dbc_WBMV_omschrijving',
                         'Betreffende Regeling': 'zpr_dbc_WMBV_regeling',
                         'Aanvullende informatie': 'zpr_dbc_WMBV_info'},
                inplace=True)
    wbmv.drop_duplicates(cols=['zpr_dbc_WBMV_code'], take_last=True,
                         inplace=True)
    wbmv = wbmv.drop(['Ingangsdatum', 'Einddatum', 'Mutatie'], axis=1)

    data = pd.merge(data, zpgo)
    data = pd.merge(data, wbmv)

    # add id, unknown and reorder
    data['zpr_id'] = range(1,len(data)+1,1)
    target_columns = get_columns(login['database'], 'DIM', 'ZORGPRODUCT', cursor)
    for column in target_columns:
        if column not in data.columns:
            data[column] = ''
    unknown = [{'zpr_id': -1,
                'zpr_dbc_zorgproduct_code': '_?_',
                'zpr_dbc_zorgproduct_omschrijving_lang': '_?_',
                'zpr_dbc_zorgproduct_omschrijving_latijn': '_?_',
                'zpr_dbc_zorgproduct_omschrijving_consument': '_?_',
                'zpr_dbc_declaratiecode_verzekerd': '_?_',
                'zpr_dbc_declaratiecode_onverzekerd': '_?_',
                'zpr_dbc_WBMV_code': '_?_',
                'zpr_dbc_WBMV_omschrijving': '_?_',
                'zpr_dbc_WMBV_regeling': '_?_',
                'zpr_dbc_WMBV_info': '_?_',
                'zpr_dbc_zorgproductgroep_code': '_?_',
                'zpr_dbc_zorgproductgroep_omschrijving': '_?_',
                'zpr_dbc_begindatum': '1000-01-01 00:00:00',
                'zpr_dbc_einddatum': '1000-01-01 00:00:00'}]
    data = data.append(pd.DataFrame(unknown), ignore_index=True)
    data = data[target_columns]
    data = data.sort(columns='zpr_id')

    # write output to .csv
    data.to_csv(paths['staging_path'] + '/DIM.ZORGPRODUCT.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='_?_')


if __name__ == '__main__':
    main()



""" Prepares DBC Tarievenlijst reference tables
for ETL in WOB_ZZ.

Following dimension tables are generated:

    DIM_ZORGPRODUCT

DIM.DECLARATIE is defined as slowly changing type 2 dimension
"""

import configparser
import pymssql as sql
import pandas as pd
import numpy as np
from decimal import Decimal
from wob_zz import parse_dates


__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


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
    + '/20140601 Tarieven Tabel 20140501.csv'
data = pd.read_csv(data_file, sep=';',dtype=str, encoding='latin1')

mapping = {
    'AGB Uitvoerder'                : 'dcl_dbc_specialisme_uitvoerend',
    'Declaratiecode'                : 'dcl_dbc_declaratie_code',
    'Omschrijving declaratiecode'   : 'dcl_dbc_omschrijving',
    'Productgroepcode'              : 'dcl_dbc_productgroep_code',
    'Tarief'                        : 'dcl_dbc_tarief',
    'Kostensoort'                   : 'dcl_dbc_kostensoort',
    'Tarieftype'                    : 'dcl_dbc_tarieftype',
    'Declaratie eenheid'            : 'dcl_dbc_declaratie_eenheid',
    'Soort Tarief'                  : 'dcl_dbc_tariefsoort',
    'Segment aanduiding'            : 'dcl_dbc_segment_aanduiding',
    'Soort Honorarium'              : 'dcl_dbc_honorariumsoort',
    'Ingangsdatum'                  : 'dcl_dbc_begindatum',
    'Einddatum'                     : 'dcl_dbc_einddatum'
}

data = data.drop(['AGB Specialisme', 'Mutatie Toelichting',
                  'Declaratie regel', 'Mutatie'], axis=1)
data.rename(columns=mapping, inplace=True)

# reformat columns
data['dcl_dbc_begindatum'] = data['dcl_dbc_begindatum'].apply(lambda x: parse_dates(x) + (' 00:00:00'))
data['dcl_dbc_einddatum'] = data['dcl_dbc_einddatum'].apply(lambda x: parse_dates(x) + (' 00:00:00'))
data['dcl_dbc_specialisme_uitvoerend'] = \
    data['dcl_dbc_specialisme_uitvoerend'].apply(lambda x: x.zfill(4))
data['dcl_dbc_tarief'] = data['dcl_dbc_tarief'].apply(lambda x: (1.0*int(x))/100)

# pivot table such that only unique specialisme_uitvoerend, declaractie_code
# items remain (with separate entries from-to date)

data = data.pivot_table(
    rows=['dcl_dbc_declaratie_code', 'dcl_dbc_omschrijving',
           'dcl_dbc_kostensoort', 'dcl_dbc_tarieftype',
           'dcl_dbc_declaratie_eenheid', 'dcl_dbc_tariefsoort',
           'dcl_dbc_segment_aanduiding', 'dcl_dbc_honorariumsoort',
           'dcl_dbc_begindatum', 'dcl_dbc_einddatum'],
    cols=['dcl_dbc_specialisme_uitvoerend'],
    values='dcl_dbc_tarief',
    aggfunc=np.max)

data = data.reset_index()


# write output to .csv
data.to_csv(paths['staging_path'] + '/DIM.DECLARATIE.csv', sep=';',
            header=True, index = False, encoding='latin-1',
            quoting=None, na_rep='_?_')
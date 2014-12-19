""" Prepares vektis COD016 reference for ETL in WOB_ZZ DWH.

Reference table of all specialisme-soorten and instelling soorten.
Manually enriched with abreviations and short descriptions
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
    paths = {'vektis_path': config.get('wob_zz', 'vektis_path'),
             'staging_path': config.get('wob_zz', 'staging_path')}
    data_file = paths['vektis_path'] + '/COD016_-_VEKT.csv'
    data = pd.read_csv(data_file, sep=';',dtype=str, encoding='cp1252',
                       parse_dates=['Mutatiedatum', 'Ingangsdatum','Expiratiedatum'])

    mapping = {
        'Waarde': 'zvs_vektis_zorgverlenersoort_code',
        'Betekenis': 'zvs_vektis_zorgverlenersoort_omschrijving',
        'Omschrijving': 'zvs_vektis_zorgverlenersoort_info1',
        'Toelichting 1': 'zvs_vektis_zorgverlenersoort_info2',
        'Toelichting 2': 'zvs_vektis_zorgverlenersoort_info3',
        'Aard mutatie': 'zvs_vektis_mutatie_aard',
        'Reden mutatie': 'zvs_vektis_mutatie_reden',
        'Mutatiedatum': 'zvs_vektis_mutatiedatum',
        'Ingangsdatum': 'zvs_vektis_begindatum',
        'Expiratiedatum': 'zvs_vektis_einddatum'
    }

    data.rename(columns=mapping, inplace=True)
    data['zvs_vektis_mutatiedatum'] = data['zvs_vektis_mutatiedatum'].apply(lambda x: datetime_to_mssql_string(x))
    data['zvs_vektis_begindatum'] = data['zvs_vektis_begindatum'].apply(lambda x: datetime_to_mssql_string(x, default='1000-01-01 00:00:00'))
    data['zvs_vektis_einddatum'] = data['zvs_vektis_einddatum'].apply(lambda x: datetime_to_mssql_string(x))

    df = [
        ('0100', 'Huisarts, nno', 'HUIS'),
        ('0101', 'Huisarts, niet apotheekhoudend', 'HUIS'),
        ('0110', 'Huisarts, apotheekhoudend', 'HUIS'),
        ('0120', 'Huisarts, alternatief', 'HUIS'),
        ('1100', 'Kaakchirurgie', 'KAAK'),
        ('1101', 'Tandartsspecialist, implantoloog', 'KAAK'),
        ('1200', 'Tandarts, algemeen', 'TAND'),
        ('1201', 'Tandarts, implantoloog', 'TAND'),
        ('1300', 'Orthodontist', 'ORTHOD'),
        ('1400', 'Arbo arts', 'ARBO'),
        ('1401', 'Arbo arts', 'ARBO'),
        ('1402', 'Arbo arts', 'ARBO'),
        ('1403', 'Arbo arts', 'ARBO'),
        ('1410', 'Arbo arts', 'ARBO'),
        ('1900', 'Audiologie', 'AUDIO'),
        ('0200', 'Apotheker', 'APOTH'),
        ('2400', 'Dietetiek', 'DIEET'),
        ('0301', 'Oogheelkunde', 'OOG'),
        ('0302', 'Keel- neus- en oorheelkunde', 'KNO'),
        ('0303', 'Chirurgie', 'CHI'),
        ('0304', 'Plastische chirurgie', 'PCH'),
        ('0305', 'Orthopedie', 'ORT'),
        ('0306', 'Urologie', 'URO'),
        ('0307', 'Gynaecologie', 'GYN'),
        ('0308', 'Neurochirurgie', 'NCH'),
        ('0309', 'Zenuw - en zielsziekten', 'ZNW'),
        ('0310', 'Dermatologie', 'DER'),
        ('0313', 'Interne geneeskunde', 'INT'),
        ('0316', 'Kindergeneeskunde', 'KIN'),
        ('0318', 'Gastro-enterologie', 'MDL'),
        ('0320', 'Cardiologie', 'CAR'),
        ('0322', 'Longgeneeskunde', 'LON'),
        ('0324', 'Reumatologie', 'REU'),
        ('0326', 'Allergologie', 'ALR'),
        ('0327', 'Revalidatie geneeskunde', 'REV'),
        ('0328', 'Cardio-pulmonale chirurgie', 'CCH'),
        ('0329', 'Consultatieve psychiatrie', 'CP'),
        ('0330', 'Neurologie', 'NEU'),
        ('0335', 'Klinische geriatrie', 'GER'),
        ('0361', 'Radiotherapie', 'RT'),
        ('0362', 'Interventie radiologie', 'RAD'),
        ('0363', 'Nucleaire geneeskunde', 'NUC'),
        ('0386', 'Klinische chemie', 'KCL'),
        ('0387', 'Medische microbiologie', 'MM'),
        ('0388', 'Pathologie', 'PATHO'),
        ('0389', 'Anaesthesiologie', 'PIJN'),
        ('0390', 'Klinische genetica', 'GEN'),
        ('0401', 'Fysiotherapie', 'FYS'),
        ('0405', 'Oedeemtherapie', 'OEDEEM'),
        ('0501', 'Logopedie', 'LOGO'),
    ]

    df = pd.DataFrame(df, columns=['zvs_vektis_zorgverlenersoort_code',
                                   'zvs_specialisme',
                                   'zvs_specialisme_afkorting'])
    data = pd.merge(data, df)

    # add id
    data['zvs_id'] = range(1,len(data)+1,1)
    target_columns = get_columns(login['database'], 'DIM', 'ZORGVERLENERSOORT', cursor)
    for column in target_columns:
        if column not in data.columns:
            data[column] = ''

    #add unknown
    unknown = [{'zvs_id': -1,
        'zvs_vektis_zorgverlenersoort_code': '_?_',
        'zvs_vektis_zorgverlenersoort_omschrijving': '_?_',
        'zvs_vektis_zorgverlenersoort_info1': '_?_',
        'zvs_vektis_zorgverlenersoort_info2': '_?_',
        'zvs_vektis_zorgverlenersoort_info3': '_?_',
        'zvs_vektis_mutatie_aard': '_?_',
        'zvs_vektis_mutatie_reden': '_?_',
        'zvs_vektis_mutatiedatum': '1000-01-01 00:00:00',
        'zvs_vektis_begindatum': '1000-01-01 00:00:00',
        'zvs_vektis_einddatum': '1000-01-01 00:00:00'}]
    data = data.append(pd.DataFrame(unknown), ignore_index=True)
    data = data[target_columns]
    data = data.sort(columns='zvs_id')

     # write output to .csv
    data.to_csv(paths['staging_path'] + '/DIM.ZORGVERLENERSOORT.csv', sep=';',
              header=True, index=False, encoding='cp1252',
              quoting=None, na_rep='_?_')


    #stage land_codes
    data_file = paths['vektis_path'] + '/COD032_-_NEN.csv'
    data = pd.read_csv(data_file, sep=';',dtype=str, encoding='cp1252')

     # write output to .csv
    data.to_csv(paths['staging_path'] + '/DIM.LAND.csv', sep=';',
              header=True, index=False, encoding='cp1252',
              quoting=None, na_rep='')

if __name__ == '__main__':
    main()
""" DDL to prepare tables on MSSQL server for ETL of WOB_ZZ DWH.

MSSQL server structure DB.SCHEMA.TABLE is used to separate
fact- and dimension tables.

Because partitioning is not available in
MSSQL server BI edition 2014 (production version), data is partioned
manually per year.

Order of columns is 'logical', i.e.
    - ID columns first
    - most important columns first

NB: no identity declaration for dimension IDs,
since this is managed by pygrametl during load.
"""

import _mssql as sql
import configparser

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

def main():
    config = configparser.ConfigParser()
    config.read('/opt/projects/wob_zz/config.ini')

    login = {
        'user': config.get('local_mssql', 'user'),
        'password': config.get('local_mssql', 'password'),
        'server': config.get('local_mssql', 'server'),
        'port': config.get('local_mssql', 'port'),
        'database': config.get('wob_zz', 'database')
    }

    # All columns that are part of unique key are non-nullable;
    # All other columns DEFAULT NULL
    tables = {}

    tables['DIM.AFSLUITREDEN'] = ('''
        create table DIM.AFSLUITREDEN (
        afs_id tinyint not null primary key,
        afs_afsluitreden_code nvarchar(2) not null unique
        )
    ''')

    tables['DIM.BEHANDELING'] = ('''
        create table DIM.BEHANDELING (
        beh_id smallint not null  primary key,
        beh_dbc_specialisme_code nvarchar(8) not null,
        beh_dbc_behandeling_code nvarchar(8) not null,
        beh_dbc_behandeling_omschrijving nvarchar(255) default null,
        beh_dbc_hoofdgroep_code nvarchar(4) default null,
        beh_dbc_hoofdgroep_omschrijving nvarchar(255) default null,
        beh_dbc_subgroep_code nvarchar(4) default null,
        beh_dbc_subgroep_omschrijving nvarchar(255) default null,
        beh_dbc_begindatum date default null,
        beh_dbc_einddatum date default null,
        constraint UQ__BEH unique (beh_dbc_specialisme_code, beh_dbc_behandeling_code)
        )
    ''')

    tables['DIM.DAG'] = ('''
        create table DIM.DAG (
        dag_id smallint not null primary key,
        dag_datum date not null unique,
        dag_jaar smallint default null,
        dag_kwartaal tinyint default null,
        dag_maand tinyint default null,
        dag_week tinyint default null,
        dag_jaar_maand nvarchar(8) default null,
        dag_jaar_week nvarchar(8) default null
        )
    ''')

    tables['DIM.DECLARATIE'] = ('''
        create table DIM.DECLARATIE (
        dcl_id smallint not null primary key,
        dcl_dbc_declaratie_code nvarchar(6) default null unique,
        dcl_dbc_tarieftype_code nvarchar(2) default null,
        dcl_dbc_tarieftype_omschrijving nvarchar(255) default null,
        dcl_dbc_declaratie_eenheid_code nvarchar(3) default null,
        dcl_dbc_declaratie_eeheid_omschrijving nvarchar(255) default null,
        dcl_dbc_tariefsoort_code nvarchar(2) default null,
        dcl_dbc_tariefsoort_omschrijving nvarchar(255) default null
        )
    ''')

    tables['DIM.DIAGNOSE'] = ('''
        create table DIM.DIAGNOSE (
        dia_id smallint not null primary key,
        dia_dbc_specialisme_code nvarchar(8) not null,
        dia_dbc_diagnose_code nvarchar(8) not null,
        dia_dbc_diagnose_omschrijving nvarchar(255) default null,
        dia_dbc_hoofdgroep_code nvarchar(4) default null,
        dia_dbc_hoofdgroep_omschrijving nvarchar(255) default null,
        dia_dbc_subgroep_code nvarchar(4) default null,
        dia_dbc_subgroep_omschrijving nvarchar(255) default null,
        dia_dbc_begindatum date default null,
        dia_dbc_einddatum date default null,
        dia_dbc_zorgproductgroep_code nvarchar(6) default null,
        dia_dbc_zorgproductgroep_omschrijving nvarchar(255) default null,
        constraint UQ__DIA unique (dia_dbc_specialisme_code, dia_dbc_diagnose_code)
        )
    ''')

    tables['DIM.LAND'] = ('''
        create table DIM.LAND (
        lnd_id tinyint not null primary key,
        lnd_land_code varchar(2) default null unique,
        lnd_land varchar(255) default null
        )
        )
    ''')

    tables['DIM.SUBTRAJECTNUMMER'] = ('''
        create table DIM.SUBTRAJECTNUMMER (
        stn_id int not null primary key,
        stn_subtraject_id nvarchar(40) not null unique,
        stn_subtrajectnummer nvarchar(30) default null,
        stn_zorgtrajectnummer nvarchar(15) default null,
        stn_zorgtrajectnummer_parent nvarchar(15) default null
        )
    ''')

    tables['DIM.ZORGPRODUCT'] = ('''
        create table DIM.ZORGPRODUCT (
        zpr_id smallint not null primary key,
        zpr_dbc_zorgproduct_code nvarchar(10) not null unique,
        zpr_dbc_zorgproduct_omschrijving_lang nvarchar(1500) default null,
        zpr_dbc_zorgproduct_omschrijving_latijn nvarchar(1500) default null,
        zpr_dbc_zorgproduct_omschrijving_consument nvarchar(1500) default null,
        zpr_dbc_WBMV_code nvarchar(4) default null,
        zpr_dbc_WBMV_omschrijving nvarchar(255) default null,
        zpr_dbc_WBMV_regeling nvarchar(255) default null,
        zpr_dbc_WBMV_info nvarchar(255) default null,
        zpr_dbc_zorgproductgroep_code nvarchar(15) default null,
        zpr_dbc_zorgproductgroep_omschrijving nvarchar(255) default null,
        zpr_dbc_begindatum date default null,
        zpr_dbc_einddatum date default null

        )
    ''')
    
    tables['DIM.ZORGTYPE'] = ('''
        create table DIM.ZORGTYPE (
        zgt_id smallint not null  primary key,
        zgt_dbc_specialisme_code nvarchar(8) not null,
        zgt_dbc_zorgtype_code nvarchar(2) not null,
        zgt_dbc_zorgtype_omschrijving nvarchar(75) default null,
        zgt_dbc_hoofdgroep_code nvarchar(4) default null,
        zgt_dbc_hoofdgroep_omschrijving nvarchar(255) default null,
        zgt_dbc_subgroep_code nvarchar(4) default null,
        zgt_dbc_subgroep_omschrijving nvarchar(255) default null,
        zgt_dbc_begindatum date default null,
        zgt_dbc_einddatum date default null
        constraint UQ__ZGT unique (zgt_dbc_specialisme_code,zgt_dbc_zorgtype_code)
        )
    ''')

    tables['DIM.ZORGVERLENERSOORT'] = ('''
        create table DIM.ZORGVERLENERSOORT (
        zvs_id smallint not null primary key,
        zvs_specialisme nvarchar(64) default null,
        zvs_specialisme_afkorting nvarchar(10) default null,
        zvs_vektis_zorgverlenersoort_code nvarchar(4) not null unique,
        zvs_vektis_zorgverlenersoort_omschrijving nvarchar(255) default null,
        zvs_vektis_zorgverlenersoort_info1 nvarchar(255) default null,
        zvs_vektis_zorgverlenersoort_info2 nvarchar(255) default null,
        zvs_vektis_zorgverlenersoort_info3 nvarchar(255) default null,
        zvs_vektis_mutatie_aard nvarchar(255) default null,
        zvs_vektis_mutatie_reden nvarchar(255) default null,
        zvs_vektis_mutatiedatum date default null,
        zvs_vektis_begindatum date default null,
        zvs_vektis_einddatum date default null
        )
    ''')

    tables['DIM.ZORGVRAAG'] = ('''
        create table DIM.ZORGVRAAG (
        zgv_id smallint not null  primary key,
        zgv_dbc_specialisme_code nvarchar(8) not null,
        zgv_dbc_zorgvraag_code nvarchar(255) not null,
        zgv_dbc_zorgvraag_omschrijving nvarchar(255) default null,
        zgv_dbc_hoofdgroep_code nvarchar(4) default null,
        zgv_dbc_hoofdgroep_omschrijving nvarchar(255) default null,
        zgv_dbc_subgroep_code nvarchar(4) default null,
        zgv_dbc_subgroep_omschrijving nvarchar(255) default null,
        zgv_dbc_begindatum date default null,
        zgv_dbc_einddatum date default null,
        constraint UQ__ZGV unique (zgv_dbc_specialisme_code,zgv_dbc_zorgvraag_code)
        )
    ''')

    """ not used; required when manually partitioning by year
    subtraject_tables = {'FCT.SUBTRAJECT_{}'.format(year): ('''
        create table FCT.SUBTRAJECT_{} (
        beh_id smallint not null default -1,
        dag_id_begindatum_zorgtraject smallint default -4,
        dag_id_einddatum_zorgtraject smallint default -4,
        dag_id_begindatum_subtraject smallint default -4,
        dag_id_einddatum_subtraject smallint default -4,
        dag_id_declaratiedatum smallint default -4,
        dia_id smallint not null default -1,
        stn_id int not null default -1,
        zgt_id smallint not null default -1,
        zgv_id smallint not null default -1,
        zpr_id int not null default -1,
        zvs_id_behandelend smallint not null default -1,
        zvs_id_verwijzend smallint not null default -1,
        geslacht tinyint default 0,
        heeft_oranje_zorgactiviteit bit default null,
        heeft_zorgactiviteit_met_machtiging bit default null,
        is_hoofdtraject bit default null,
        is_aanspraak_zvw bit default null,
        is_aanspraak_zvw_toegepast bit default null,
        is_zorgactiviteitvertaling_toegepast bit default null,
        fct_omzet_ziekenhuis decimal(9,2) default null,
        fct_omzet_honorarium_totaal decimal(9,2) default null
        )
    ''').format(year) for year in range(2009,2015,1)}
    """

    tables['FCT.SUBTRAJECT'] = ('''
        create table FCT.SUBTRAJECT (
        beh_id smallint not null default -1,
        dag_id_begindatum_zorgtraject smallint default -4,
        dag_id_einddatum_zorgtraject smallint default -4,
        dag_id_begindatum_subtraject smallint default -4,
        dag_id_einddatum_subtraject smallint default -4,
        dag_id_declaratiedatum smallint default -4,
        dia_id smallint not null default -1,
        stn_id int not null default -1,
        zgt_id smallint not null default -1,
        zgv_id smallint not null default -1,
        zpr_id int not null default -1,
        zvs_id_behandelend smallint not null default -1,
        zvs_id_verwijzend smallint not null default -1,
        geslacht tinyint default 0,
        heeft_oranje_zorgactiviteit bit default null,
        heeft_zorgactiviteit_met_machtiging bit default null,
        is_hoofdtraject bit default null,
        is_aanspraak_zvw bit default null,
        is_aanspraak_zvw_toegepast bit default null,
        is_zorgactiviteitvertaling_toegepast bit default null,
        fct_omzet_ziekenhuis decimal(9,2) default null,
        fct_omzet_honorarium_totaal decimal(9,2) default null
        )
    ''')




    cnx = sql.connect(**login)

    for name, ddl in tables.items():
        try:
            print('Dropping if exists and creating table {}: '.format(name), end='')
            stmt = "if (exists (select * from information_schema.tables " \
                   "where table_schema = '{}' " \
                   "and table_name = '{}')) " \
                   "drop table {}"
            cnx.execute_non_query(stmt.format(name.split('.')[0], name.split('.')[1], name))
            cnx.execute_non_query(ddl)
        except sql.MSSQLDatabaseException as error:
            raise
            print(error.message)
        else:
            print('OK')

    cnx.close()

if __name__ == '__main__':
    main()

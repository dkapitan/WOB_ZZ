""" Generate stg data for prefilling dimension tables.

Period 2007-01-01 to 2020-12-31.
dag_id -1,-2,-3,-4 for onbekende, nvt, foute and open dates

"""

from datetime import date
from dateutil.rrule import rrule, DAILY
import pandas as pd
import configparser

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

config = configparser.ConfigParser()
config.read('/opt/projects/wob_zz/config.ini')
staging_path = config.get('wob_zz', 'staging_path')

def main():
    start_date = date(2007, 1, 1)
    end_date = date(2020, 12, 31)

    row_list = []

    # standard values for onbekende, not relevant, wrong domain value, open dates
    datum_onbekend = {'dag_id': -1, 'dag_datum': '1000-01-01 00:00:00',
                      'dag_week': 1, 'dag_kwartaal': 1, 'dag_maand': 1,
                      'dag_jaar': '1000', 'dag_jaar_week': '1000-01',
                      'dag_jaar_maand': '1000-12'}
    row_list.append(datum_onbekend)

    datum_nvt = {'dag_id': -2, 'dag_datum': '1000-02-02 00:00:00',
                 'dag_week': 2, 'dag_maand': 2, 'dag_kwartaal': 2,
                 'dag_jaar': '1000', 'dag_jaar_week': '1000-02',
                 'dag_jaar_maand': '1000-02'}
    row_list.append(datum_nvt)

    datum_foute_waarde = {'dag_id': -3, 'dag_datum': '1000-03-03 00:00:00',
                          'dag_week': 3, 'dag_maand': 3, 'dag_kwartaal': 3,
                          'dag_jaar': '1000', 'dag_jaar_week': '1000-03',
                          'dag_jaar_maand': '1000-03'}
    row_list.append(datum_foute_waarde)

    datum_open = {'dag_id': -4, 'dag_datum': '1000-04-04 00:00:00',
                  'dag_week': 4, 'dag_maand': 4, 'dag_kwartaal': 4,
                  'dag_jaar': '1000', 'dag_jaar_week': '1000-04',
                  'dag_jaar_maand': '1000-04'}

    row_list.append(datum_open)

    id = 0
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        id += 1
        days = {}
        cldr = dt.isocalendar()
        days.update({'dag_id': id})
        days.update({'dag_datum': str(dt)})
        days.update({'dag_week': cldr[1]})
        days.update({'dag_maand': dt.month})
        days.update({'dag_kwartaal': str((dt.month-1) // 3 + 1)})
        days.update({'dag_jaar': dt.year})
        yr = dt.year
        wk = cldr[1]
        mnth = dt.month
        days.update({'dag_jaar_week': str(yr) + '-' + str(wk)})
        days.update({'dag_jaar_maand': str(yr) + '-' + str(mnth)})
        row_list.append(days)

    stg_dag = pd.DataFrame(row_list)
    stg_dag = stg_dag[['dag_id', 'dag_datum', 'dag_jaar', 'dag_kwartaal',
                       'dag_maand', 'dag_week', 'dag_jaar_maand',
                       'dag_jaar_week']]
    stg_dag.to_csv(staging_path + '/DIM.DAG.csv', sep=';',
                   header=True, index=False, encoding='utf-8',
                   line_terminator='\r\n')

if __name__ == '__main__':
    main()

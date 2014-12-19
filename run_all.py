#!/usr/bin/env python
""" Script to run whole ETL for WOB_ZZ.

All steps are run sequentially, parallel loading may be developed
in the future.
"""

import wob_zz

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

if __name__  == '__main__':
    wob_zz.create_tables.main()
    wob_zz.stage_date_dimensions.main()
    wob_zz.stage_dbc_typeringslijst.main()
    wob_zz.stage_dbc_zorgproduct.main()
    wob_zz.stage_vektis_codelijsten.main()
    wob_zz.load_staged_dimensions.main()
    wob_zz.load_fct_subtraject.main()

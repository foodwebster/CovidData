# -*- coding: utf-8 -*-

'''
script for preparing data for use in dashboard
'''

import numpy as np
import pandas as pd
from datetime import datetime

from prepare_case_data import get_case_data
from prepare_movement_data import get_movement_data
from prepare_demog_data import get_demog_data

def load_data():
    start_date = datetime(2020, 2, 15)
    
    country_case_df, state_case_df, county_case_df = get_case_data(start_date)
    country_mmt_df, state_mmt_df, county_mmt_df = get_movement_data(start_date)
    country_demog_df, state_demog_df, county_demog_df = get_demog_data()
    
    dropcol = ['country_region_code', 'country_region', 'sub_region_1', 'sub_region_2']
    country_df = country_case_df.join(country_mmt_df.drop(columns=dropcol), how='left')
    country_df = pd.concat([country_df, 
                            pd.DataFrame(np.repeat(country_demog_df.to_numpy(), len(country_df), axis=0), 
                                         columns=country_demog_df.columns,
                                         index=country_df.index)
                           ], axis=1)
    
    state_mmt_df = state_mmt_df.drop(columns=dropcol + ['State']) \
                               .rename(columns={'Abbreviation': 'State'}) \
                               .set_index('State', append=True)
    state_df = state_case_df.set_index('State', append=True) \
                            .join(state_mmt_df, how='left') \
                            .reset_index(level='State') \
                            .sort_index() \
                            .reset_index()
    state_df = state_df.merge(state_demog_df, on='State')
    
    # drop cruise ship fake county
    county_case_df = county_case_df[county_case_df.countyFIPS != 6000]
    # remove unneeded columns and set index to fips
    county_case_df = county_case_df.drop(columns=['stateFIPS', 'countyFIPS']) \
                                   .set_index('fips_str', append=True)
    county_mmt_df = county_mmt_df.drop(columns=['country_region_code', 
                                                 'country_region', 
                                                 'sub_region_1', 
                                                 'sub_region_2',
                                                 'State_x', 
                                                 'Abbreviation', 
                                                 'FIPS', 
                                                 'Name', 
                                                 'State_y']) \
                                   .set_index('fips_str', append=True)
    county_df = county_case_df.join(county_mmt_df, how='outer') \
                              .reset_index()
    county_df = county_df.merge(county_demog_df, on='fips_str', how='left')
     
    # add missing movement data at end of each county's time series
    for county in county_df.fips_str.unique():
        county_mask = county_df.fips_str == county
        opc = county_df.loc[county_mask, 'overall_pct_change']
        county_df.loc[county_mask, 'overall_pct_change'] = opc.fillna(opc.mean()).fillna(0)

    # fill null values
    final_attr = ['AREA_SQMI', 'White',
       'IncomePerCap', 'Poverty', 'ChildPoverty',
       'Service', 'Production', 'dem_gop_diff_16','FBFilterBubble'
       ]
    for attr in final_attr:
        county_df[attr] = county_df[attr].fillna(country_df[attr]).fillna(0)
        state_df[attr] = state_df[attr].fillna(country_df[attr]).fillna(0)

    
    return country_df.reset_index(), state_df, county_df


country_df, state_df, county_df = load_data()

country_df.to_csv("CountryData.csv", index=False)
state_df.to_csv("StateData.csv", index=False)
county_df.to_csv("CountyData.csv", index=False)

to_dashboard = True
if to_dashboard:
    db = "../CovidDashboard/data/"
    country_df.to_csv(db + "CountryData.csv", index=False)
    state_df.to_csv(db + "StateData.csv", index=False)
    county_df.to_csv(db + "CountyData.csv", index=False)

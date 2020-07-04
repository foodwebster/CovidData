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
    '''
    Load the latest data from multiple sources - covid case data, movement data 
    and demographics, at country, stte and county levels.
    
    Re-arrange data to produce country, state and county-level datasets that 
    combine covid case, movement and demographic data
    '''
    
    start_date = datetime(2020, 2, 15)
    
    # covid case data
    country_case_df, state_case_df, county_case_df = get_case_data(start_date)
    # google mobility data
    country_mmt_df, state_mmt_df, county_mmt_df = get_movement_data(start_date)
    # county-level demographics
    country_demog_df, state_demog_df, county_demog_df = get_demog_data()
    
    # make country time series data - join case and movement data
    dropcol = ['country_region_code', 'country_region', 'sub_region_1', 'sub_region_2']
    country_df = country_case_df.join(country_mmt_df.drop(columns=dropcol), how='left')
    # add demographics
    country_df = pd.concat([country_df, 
                            pd.DataFrame(np.repeat(country_demog_df.to_numpy(), len(country_df), axis=0), 
                                         columns=country_demog_df.columns,
                                         index=country_df.index)
                           ], axis=1)
    
    # make state time series data
    state_mmt_df = state_mmt_df.drop(columns=dropcol + ['State']) \
                               .rename(columns={'Abbreviation': 'State'}) \
                               .set_index('State', append=True)
    state_df = state_case_df.set_index('State', append=True) \
                            .join(state_mmt_df, how='left') \
                            .reset_index(level='State') \
                            .sort_index() \
                            .reset_index()
    # add state demographics
    state_df = state_df.merge(state_demog_df, on='State')
    
    # make county time series data
    # first drop cruise ship fake county
    county_case_df = county_case_df[county_case_df.countyFIPS != 6000]
    # in covid case data, remove unneeded columns and set index to fips (county code)
    county_case_df = county_case_df.drop(columns=['stateFIPS', 'countyFIPS']) \
                                   .set_index('fips_str', append=True)
    # keep only needed columns in the movement data
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
    # join case and movement data
    county_df = county_case_df.join(county_mmt_df, how='outer') \
                              .reset_index()
    # add county-level demographic data
    county_df = county_df.merge(county_demog_df, on='fips_str', how='left')
    # movement dataset lags current date - 
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


# Get the latest data
country_df, state_df, county_df = load_data()
    
# Save final datasets to csv files
country_df.to_csv("CountryData.csv", index=False)
state_df.to_csv("StateData.csv", index=False)
county_df.to_csv("CountyData.csv", index=False)

# Optionally copy the data to the dashboard folder
to_dashboard = True
if to_dashboard:
    db = "../CovidDashboard/data/"
    country_df.to_csv(db + "CountryData.csv", index=False)
    state_df.to_csv(db + "StateData.csv", index=False)
    county_df.to_csv(db + "CountyData.csv", index=False)

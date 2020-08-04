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
from get_employment_data import get_monthly_county_employment_data
from get_real_estate_data import get_zillow_data

def get_latest_data():
    '''
    Load the latest data from multiple sources - covid case data, movement data 
    and demographics, at country, stte and county levels.
    
    Re-arrange data to produce country, state and county-level datasets that 
    combine covid case, movement and demographic data
    '''
    
    start_date = datetime(2020, 2, 15)
    
    # covid case data
    print("Getting case data")
    country_case_df, state_case_df, county_case_df = get_case_data(start_date)
    # google mobility data
    print("Getting mobility data")
    country_mmt_df, state_mmt_df, county_mmt_df = get_movement_data(start_date)
    # county-level demographics
    print("Getting demographic data")
    country_demog_df, state_demog_df, county_demog_df = get_demog_data()
    # employment data
    print("Getting unemployment data")
    country_edf, state_edf, county_edf = get_monthly_county_employment_data()
    # zillow home price and rental index data
    country_zdf, state_zdf, county_zdf = get_zillow_data()
    # build state and national data by population averaging across counties
    

    print("Merging datasets")    
    # make country time series data - join case and movement data
    dropcol = ['country_region_code', 'country_region', 'sub_region_1', 'sub_region_2']
    country_df = country_case_df.join(country_mmt_df.drop(columns=dropcol), how='left')
    # add employment
    country_df = country_df.join(country_edf['UnempRate'], how='left')
    # add zillow
    #country_df = country_df.merge(country_zdf, on=['date'], how='left')
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
    # add employment
    state_df = state_df.merge(state_edf[['State', 'date', 'UnempRate']], on=['State', 'date'], how='left')
    # add zillow
    #state_df = state_df.merge(state_zdf, on=['State', 'date'], how='left')
    # add state demographics
    state_df = state_df.merge(state_demog_df, on='State')
    
    # make county time series data
    # first drop cruise ship fake county
    county_case_df = county_case_df[county_case_df.countyFIPS != 6000]
    # in covid case data, remove unneeded columns
    county_case_df = county_case_df.drop(columns=['stateFIPS', 'countyFIPS'])
    # keep only needed columns in the movement data
    county_mmt_df = county_mmt_df.drop(columns=['country_region_code', 
                                                 'country_region', 
                                                 'sub_region_1', 
                                                 'sub_region_2',
                                                 'State_x', 
                                                 'Abbreviation', 
                                                 'Name', 
                                                 'State_y'])
    # merge case and movement data
    county_df = county_case_df.reset_index().merge(county_mmt_df.reset_index(), on=['date', 'FIPS'], how='outer')
    # add employment
    county_df = county_df.merge(county_edf[['FIPS', 'date', 'UnempRate']], on=['FIPS', 'date'], how='left')
    # add zillow
    #county_df = county_df.merge(county_zdf[['FIPS', 'date', 'rent_index', 'home_value']], on=['FIPS', 'date'], how='left')
    # add county-level demographic data
    county_df = county_df.merge(county_demog_df, on='FIPS', how='left')
    # movement dataset lags current date - 
    # add missing movement data at end of each county's time series
    print("Filling missing data")
    for county in county_df.FIPS.unique():
        county_mask = county_df.FIPS == county
        opc = county_df.loc[county_mask, 'overall_pct_change']
        opc.fillna(opc.mean(), inplace=True)
        opc.fillna(0, inplace=True)
    # fill null values with country value
    final_attr = ['AREA_SQMI', 'White',
       'IncomePerCap', 'Poverty', 'ChildPoverty',
       'Service', 'Production', 'dem_gop_diff_16','FBFilterBubble',
       #'UnEmpRate', 
       #'rent_index', 'home_value'
       ]
    for attr in final_attr:
        county_df[attr] = county_df[attr].fillna(country_df[attr]).fillna(0)
        state_df[attr] = state_df[attr].fillna(country_df[attr]).fillna(0)

    return country_df.reset_index(), state_df, county_df


# Get the latest data
country_df, state_df, county_df = get_latest_data()
    
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

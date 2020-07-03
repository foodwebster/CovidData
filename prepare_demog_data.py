# -*- coding: utf-8 -*-

import pandas as pd

import common as cmn

def get_demog_data(start_date=None):
    def compute_attr(df):
        df.IncomePerCap = df.IncomePerCap / df.TotalPop
        df.dem_gop_diff_16 = df.dem_gop_diff_16 / df.TotalPop

        df.White = 100 * df.White / df.TotalPop
        df.Poverty = 100 * df.Poverty / df.TotalPop
        df.ChildPoverty = 100 * df.ChildPoverty / df.TotalPop
        df.Service = 100 * df.Service / df.TotalPop
        df.Production = 100 * df.Production / df.TotalPop
        
    
    demog_file = cmn.datapath/'CountyDemographics2015.xlsx'
    demog_df = pd.read_excel(demog_file)
    demog_df['fips_str'] = demog_df.id.apply(lambda x: "%05d"%x)
    
    demog_attr = ['AREA_SQMI', 'TotalPop', 'White',
       'IncomePerCap', 'Poverty', 'ChildPoverty', 'Service', 'Production',
       'dem_gop_diff_16', 'FBFilterBubble'
       ]
    all_attr = ['STATE', 'fips_str'] + demog_attr
    demog_df = demog_df[all_attr]
    
    demog_df.IncomePerCap = demog_df.IncomePerCap * demog_df.TotalPop
    demog_df.dem_gop_diff_16 = demog_df.dem_gop_diff_16 * demog_df.TotalPop

    demog_df.White = demog_df.White * demog_df.TotalPop  / 100
    demog_df.Poverty = demog_df.Poverty * demog_df.TotalPop  / 100
    demog_df.ChildPoverty = demog_df.ChildPoverty * demog_df.TotalPop  / 100
    demog_df.Service = demog_df.Service * demog_df.TotalPop  / 100
    demog_df.Production = demog_df.Production * demog_df.TotalPop  / 100
    demog_df.dem_gop_diff_16 = -demog_df.dem_gop_diff_16

    # build state-level aggregates
    state_df = demog_df.groupby('STATE')[demog_attr].sum()
    # build country-level aggregates
    country_df = pd.DataFrame([state_df.sum(axis=0)])
                       
    state_df = state_df.reset_index().rename(columns={'STATE': 'State'})
    
    # build country-level aggregates
    country_df = pd.DataFrame([state_df.sum(axis=0)])
    
    final_attr = ['AREA_SQMI', 'White',
       'IncomePerCap', 'Poverty', 'ChildPoverty',
       'Service', 'Production', 'dem_gop_diff_16','FBFilterBubble'
       ]
    compute_attr(demog_df)
    demog_df = demog_df[['fips_str'] + final_attr]
    
    compute_attr(state_df)
    state_df = state_df[['State'] + final_attr]

    compute_attr(country_df)
    
    # fill null values
    for attr in final_attr:
        demog_df[attr].fillna(country_df[attr], inplace=True)
        state_df[attr].fillna(country_df[attr], inplace=True)
    
    return country_df, state_df, demog_df

if __name__ == "__main__":
    # execute only if run as a script
    country_df, state_df, county_df = get_demog_data()
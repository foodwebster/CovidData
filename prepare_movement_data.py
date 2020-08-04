# -*- coding: utf-8 -*-

import pandas as pd

import common as cmn

def get_movement_data(start_date=None):

    def smooth_data(df, attr, grp):
        if grp is None:
             df[attr] = df[attr].rolling(7).mean().fillna(0)
        else:
            for idx, val in enumerate(df[grp].unique()):
                if idx % 100 == 0:
                    print("Smoothing movement data for %s %s"%(grp, idx))
                mask = (df[grp] == val)
                new_vals = df.loc[mask, attr].rolling(7).mean()
                # fill initial unsmoothed values
                new_vals.iloc[0:6] = df.loc[mask, attr].iloc[0:6]
                df.loc[mask, attr] = new_vals
            df[attr].fillna(0, inplace=True)
            
    #google_file = cmn.datapath/'Google_US_Mobility_Report.csv'
    google_url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
    
    abbr_file = cmn.datapath/'state_abbreviations.csv'
    # get the google movement data, fips and state abbreviations
    df = pd.read_csv(google_url)
    fips_df = cmn.get_fips()
    abbr_df = pd.read_csv(abbr_file)
    
    # strip non-US data from google file
    df = df[df.country_region_code == 'US']
    
    # convert dates
    df.date = pd.to_datetime(df.date)
    if start_date is not None:
        df = df[df.date >= start_date]
    # strip 'County' from end of county names
    df.sub_region_2 = df.sub_region_2.str.replace(' County', '')
    # add computed overall feature
    overall = 'overall_pct_change'
    df[overall] = df[['retail_and_recreation_percent_change_from_baseline',
       'grocery_and_pharmacy_percent_change_from_baseline',
       'parks_percent_change_from_baseline',
       'transit_stations_percent_change_from_baseline',
       'workplaces_percent_change_from_baseline',
       'residential_percent_change_from_baseline']].mean(axis=1)
    # get country-level records
    country_df = df[df.sub_region_1.isnull() & df.sub_region_2.isnull()]
    # add state abbreviation
    df = df.merge(abbr_df, left_on='sub_region_1', right_on='State')
    # get state data
    state_df = df[(~df.sub_region_1.isnull()) & df.sub_region_2.isnull()]
    # add fips    
    county_df = df.merge(fips_df, left_on=['Abbreviation', 'sub_region_2'], right_on=['State', 'Name'])
    county_df['FIPS'] = county_df.FIPS.apply(lambda x: "%05d"%x)
    
    print("Smoothing movement data")
    smooth_data(county_df, overall, 'FIPS')
    smooth_data(state_df, overall, 'State')
    smooth_data(country_df, overall, None)
    
    return country_df.set_index('date'), state_df.set_index('date'), county_df.set_index('date')

if __name__ == "__main__":
    # execute only if run as a script
    country_df, state_df, county_df = get_movement_data()
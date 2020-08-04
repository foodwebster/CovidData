# -*- coding: utf-8 -*-

import pandas as pd
import common as cmn

def get_testing_data():
    def clean_data(df):
        df.date = pd.to_datetime(state_df.date, format="%Y%m%d")
        df.rename(columns={'state': 'State'})
        df['dailyFracPositive'] = df.positiveIncrease/df.totalTestResultsIncrease
        df['fracPositive'] = df.positive/df.totalTestResults
        
    attr = ['date', 'state', 'positive', 'positiveIncrease', 
            'totalTestResults', 'totalTestResultsIncrease']
        
    us_url = 'https://covidtracking.com/api/v1/us/daily.csv'
    state_url = 'https://covidtracking.com/api/v1/states/daily.csv'
    country_df = pd.read_csv(us_url)[attr]
    clean_data(country_df)
    state_df = pd.read_csv(state_url)[attr]
    clean_data(state_df)
    # no county-level testing data - replicate state rates into counties
    pop_df = cmn.get_county_pop_data()
    pop_totals = pop_df.groupby('State')['population'].sum()
    pop_df['state_pop'] = pop_df.State.map(pop_totals)
    pop_df['popFrac'] = pop_df.population/pop_df.state_pop
    fips_df = cmn.get_fips()
    
    return state_df, country_df
    
if __name__ == "__main__":
    # execute only if run as a script
    state_df, country_df = get_testing_data()
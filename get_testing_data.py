# -*- coding: utf-8 -*-

import pandas as pd

def get_testing_data():
    us_url = 'https://covidtracking.com/api/v1/us/daily.csv'
    state_url = 'https://covidtracking.com/api/v1/states/daily.csv'
    country_df = pd.read_csv(us_url)
    state_df = pd.read_csv(state_url)
    return state_df, country_df
    
if __name__ == "__main__":
    # execute only if run as a script
    state_df, country_df = get_testing_data()
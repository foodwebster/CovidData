# -*- coding: utf-8 -*-

import pathlib as pl
import pandas as pd

datapath = pl.Path('raw_data')

def get_fips():
    fips_file = datapath/'fips_codes.csv'
    return pd.read_csv(fips_file)

def get_county_pop_data():
    county_pop_url = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_county_population_usafacts.csv"
    return pd.read_csv(county_pop_url).drop_duplicates(subset='countyFIPS')


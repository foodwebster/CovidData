import datetime as dt
import numpy as np
import pandas as pd

import common as cmn

# get monthly county employment data, interpolated to daily
def get_monthly_county_employment_data():
    def to_int(str_vals):
        str_vals.fillna('0', inplace=True)
        return str_vals.str.strip().str.replace(',', '').apply(lambda x: int(x) if x != '-' else 0)
    
    fips_df = pd.read_csv(cmn.datapath/'fips_codes.csv')
    fips_df.set_index('FIPS', inplace=True)
    url = 'https://www.bls.gov/web/metro/laucntycur14.txt'
    df = pd.read_csv(url, 
                     sep='|', 
                     skiprows = 6,
                     names=['LAUS', 'StateFIPS', 'CountyFIPS', 'Name', 'Period', 'LaborForce', 'Employed', 'Unemployed', 'UnempRate'],
                     dtype=str)
    df = df[df.LAUS.str.strip().str.startswith('CN')]
    df['FIPS'] = df.StateFIPS.str.strip() + df.CountyFIPS.str.strip()
    df['State'] = df.FIPS.astype(int).apply(lambda x: fips_df.State.loc[x] if x in fips_df.index else '')
    df.LaborForce = to_int(df.LaborForce)
    df.Employed = to_int(df.Employed)
    df.Unemployed = to_int(df.Unemployed)
    df.UnempRate = df.UnempRate.str.strip().apply(lambda x: float(x) if x != '-' else np.nan)
    df['date'] = pd.to_datetime(df.Period.str.strip().str.rstrip('(p)'), format='%b-%y')
    df = df.drop(columns=['LAUS', 'StateFIPS', 'CountyFIPS', 'Name', 'Period'])
    df = df.dropna(subset=['UnempRate'])
    # drop pre-2020 dates
    df = df[df.date >= dt.datetime(2020, 1, 1)]
    # add days and interpolate
    df.set_index('date', inplace=True)
    daily_dfs = []
    for idx, fips in enumerate(df.FIPS.unique()):
        if idx%100 == 0:
            print("Processing %d"%idx)
        fdf = df[df.FIPS == fips]
        days = pd.date_range(start=fdf.index.min(), end=fdf.index.max(), freq='D')
        new_df = fdf.reindex(days)
        new_df = new_df.interpolate(method='index')
        new_df.FIPS = fips
        daily_dfs.append(new_df)
    edf = pd.concat(daily_dfs).reset_index().rename(columns={'index': 'date'})
    state_edf = edf.groupby(['State', 'date']).agg({'Employed': 'sum', 'Unemployed': 'sum', 'LaborForce': 'sum'})
    state_edf['UnempRate'] = state_edf.Unemployed/state_edf.LaborForce
    country_edf = state_edf.groupby('date').agg({'Employed': 'sum', 'Unemployed': 'sum', 'LaborForce': 'sum'})
    country_edf['UnempRate'] = country_edf.Unemployed/country_edf.LaborForce
    return country_edf, state_edf, edf

# weekly unemployment claims
# https://oui.doleta.gov/unemploy/claims.asp


#national_unemp_rate, state_edf, county_edf = get_monthly_county_employment_data()
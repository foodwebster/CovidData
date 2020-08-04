# -*- coding: utf-8 -*-

import pandas as pd
import common as cmn

def get_case_data(start_date=None):
    
    cases_url = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv"
    deaths_url = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_deaths_usafacts.csv"

    def compute_stats(df, grp):
        '''
        grp - grouping attribute (state or county)
        '''
        if grp == None:
            df['new_cases'] = (df['cases'] - df['cases'].shift(1, fill_value=0)).rolling(7).mean().fillna(0)
            df['new_deaths'] = df['deaths'] - df['deaths'].shift(1, fill_value=0).rolling(7).mean().fillna(0)
        else:
            df['new_cases'] = 0.0
            df['new_deaths'] = 0.0
            for idx, val in enumerate(df[grp].unique()):
                if idx % 100 == 0:
                    print("Computing %s stats for %d"%(grp, idx))
                # compute new cases/deaths in one group (state, county) at a time
                mask = df[grp] == val
                cases = df.loc[mask, 'cases']
                df.loc[mask, 'new_cases'] = (cases - cases.shift(1, fill_value=0)).rolling(7).mean()
                deaths = df.loc[mask, 'deaths']
                df.loc[mask, 'new_deaths'] = (deaths - deaths.shift(1, fill_value=0)).rolling(7).mean()
            df.new_cases.fillna(0, inplace=True)
            df.new_deaths.fillna(0, inplace=True)
            # fix occasional data error where total case/death  count declines
            df.new_cases.loc[df.new_cases < 0] = 0
            df.new_deaths.loc[df.new_deaths < 0] = 0
        has_pop = df.population > 0
        df['cases_per_100k'] = df['deaths_per_100k'] = df['new_cases_per_100k'] = 0
        df.loc[has_pop, 'cases_per_100k'] = 1e5 * df.loc[has_pop, 'cases']/df.population[has_pop]
        df.loc[has_pop, 'deaths_per_100k'] = 1e5 * df.loc[has_pop, 'deaths']/df.population[has_pop]
        df.loc[has_pop, 'new_cases_per_100k'] = 1e5 * df.loc[has_pop, 'new_cases']/df.population[has_pop]
        df.loc[has_pop, 'new_deaths_per_100k'] = 1e5 * df.loc[has_pop, 'new_deaths']/df.population[has_pop]
        
    print("Getting case data")
    cases_df = pd.read_csv(cases_url)
    cases_df = cases_df.melt(id_vars=['countyFIPS', 'County Name', 'State', 'stateFIPS'], 
                             var_name="date", 
                             value_name="cases")
    # remove Grand Princess data that has a fake county id
    cases_df = cases_df[cases_df.countyFIPS != 6000]
    # set state, county, date hierarchical index
    cases_df = cases_df.set_index(['stateFIPS', 'countyFIPS', 'date'])
    
    print("Getting deaths data")
    deaths_df = pd.read_csv(deaths_url)
    deaths_df = deaths_df.melt(id_vars=['countyFIPS', 'County Name', 'State', 'stateFIPS'], 
                               var_name="date", 
                               value_name="deaths").set_index(['stateFIPS', 'countyFIPS', 'date'])
    df = cases_df.join(deaths_df[['deaths']]).reset_index()
    
    print("Adding population data")
    # merge county populations into case data
    pop_df = cmn.get_county_pop_data()
    df = df.merge(pop_df[['countyFIPS', 'population']], on='countyFIPS')
    # keep records with a county identifier
    df = df[df.countyFIPS > 0]
    df['FIPS'] = df.countyFIPS.apply(lambda x: "%05d"%x)
    # keep records at or after start date
    df.date = pd.to_datetime(df.date)
    if start_date is not None:
        df = df[df.date >= start_date]

    print("Building state-level aggregates")
    # build state-level aggregates
    state_pop_df = pop_df.groupby('State').agg({'population': 'sum'})
    state_df = df.groupby(['State', 'date']).agg({'cases': 'sum', 'deaths': 'sum'}).reset_index()
    state_df = state_df.merge(state_pop_df, on='State')
    
    # build country-level aggregates
    print("Building national-level aggregates")
    country_df = state_df.groupby(['date']).agg({'cases': 'sum', 'deaths': 'sum'})
    country_df['population'] = state_pop_df.population.sum()
    
    # add computed features
    print("Adding computed features")
    compute_stats(country_df, None)
    compute_stats(state_df, 'State')
    compute_stats(df, 'FIPS')

    return country_df, state_df.set_index('date'), df.set_index('date')

if __name__ == "__main__":
    # execute only if run as a script
    country_df, state_df, county_df = get_case_data()
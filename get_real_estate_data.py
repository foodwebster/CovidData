# -*- coding: utf-8 -*-

import pandas as pd
import common as cmn

def get_zillow_data():
    pop_df = cmn.get_county_pop_data()
    pop_df['FIPS'] = pop_df.countyFIPS.apply(lambda x: '%05d'%x)
    
    zillow_home_value = 'http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv'
    zillow_rental_index = 'http://files.zillowstatic.com/research/public_v2/zori/Zip_ZORI_AllHomesPlusMultifamily_SSA.csv'
    
    fips_df = pd.read_csv(cmn.datapath/'fips_codes.csv')
    fips_df['FIPS'] = fips_df.FIPS.apply(lambda x: "%05d"%x)
    
    zf = pd.read_csv(cmn.datapath/'ZipFips.csv').set_index('ZIP')
    zhv_df = pd.read_csv(zillow_home_value)
    zri_df = pd.read_csv(zillow_rental_index)
    
    #rental data is by zipcode, convert to by county (fips)
    zri_df['fips'] = zri_df.RegionName.apply(lambda x: zf.FIPS.loc[[x]].to_numpy())
    zri_df = zri_df.explode('fips').drop(columns=['RegionID', 'RegionName', 'SizeRank'])
    # there can be multiple records for each fips as a county can have more than one zipcode
    # simplest is just to average the index over each zipcode in a county, this ignores size variations
    zri_df = zri_df.groupby('fips').mean()
    # clean up, drop unused columns and stack columns to rows
    zri_df = zri_df.drop(columns=[col for col in zri_df.columns if not col.startswith('2020')])
    zri_df = zri_df.stack().reset_index()
    zri_df.columns = ['fips', 'date', 'rent_index']
    # convert fips to string
    zri_df['FIPS'] = zri_df.fips.apply(lambda x: "%05d"%x)
    zri_df.date = pd.to_datetime(pd.to_datetime(zri_df.date) + pd.offsets.MonthEnd(0))
    # keep only fips string, date and rent index
    zri_df = zri_df[['FIPS', 'date', 'rent_index']]
    
    # clean and reformat home value data - remove unused columns and stack columns to rows
    # set fips as index
    zhv_df['FIPS'] = zhv_df.StateCodeFIPS.apply(lambda x: "%02d"%x) + zhv_df.MunicipalCodeFIPS.apply(lambda x: "%03d"%x) 
    zhv_df = zhv_df.set_index('FIPS')
    # drop unneeded columns
    zhv_df = zhv_df.drop(columns=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'State', 'Metro'])
    zhv_df = zhv_df.drop(columns=[col for col in zhv_df.columns if not col.startswith('2020')])
    # stack the data - columns are dates, will become rows
    zhv_df = zhv_df.stack().reset_index()
    # rename the columns
    zhv_df.columns = ['FIPS', 'date', 'home_value']
    # convert date string to datetime
    zhv_df.date = pd.to_datetime(zhv_df.date)
    
    # merge home price and rental index data to produce the county-level datset
    zdf = zri_df.merge(zhv_df, on=['FIPS', 'date'], how='outer')

    # add days and interpolate to get daily data for each county
    zdf.set_index('date', inplace=True)
    daily_dfs = []
    for idx, fips in enumerate(zdf.FIPS.unique()):
        if idx%100 == 0:
            print("Interpolating real estate data for county %d"%idx)
        fdf = zdf[zdf.FIPS == fips]
        days = pd.date_range(start=fdf.index.min(), end=fdf.index.max(), freq='D')
        new_df = fdf.reindex(days)
        new_df = new_df.interpolate(method='index')
        new_df.FIPS = fips
        daily_dfs.append(new_df)
    zdf = pd.concat(daily_dfs).reset_index().rename(columns={'index': 'date'})
    
    #
    # compute state and country indices by computing population-weighted means of the county-level data
    #
    # merge in county population data
    _zdf = zdf.merge(pop_df[['FIPS', 'population']], on='FIPS')
    # add states
    _zdf = _zdf.merge(fips_df[['FIPS', 'State']], on='FIPS')
    # get separate home and rental data with no missing data
    ri_df = _zdf.dropna(subset=['rent_index']).copy()
    hv_df = _zdf.dropna(subset=['home_value']).copy()
    # compute value-population product for weighted means
    ri_df['pop_ri'] = ri_df.population * ri_df.rent_index
    hv_df['pop_hv'] = hv_df.population * hv_df.home_value

    # compute weighted mean rent_index for states
    state_ri_df = ri_df.groupby(['State', 'date'])[['pop_ri', 'population']].sum().reset_index()
    state_ri_df['rent_index'] = state_ri_df.pop_ri/state_ri_df.population
    # and for home value
    state_hv_df = hv_df.groupby(['State', 'date'])[['pop_hv', 'population']].sum().reset_index()
    state_hv_df['home_value'] = state_hv_df.pop_hv/state_hv_df.population
    
    state_zdf = state_ri_df[['date', 'State', 'rent_index']].merge(state_hv_df[['date', 'State', 'home_value']], on=['date', 'State'])
    
    # compute weighted mean rent_index for country
    country_ri_df = ri_df.groupby(['date'])[['pop_ri', 'population']].sum().reset_index()
    country_ri_df['rent_index'] = country_ri_df.pop_ri/country_ri_df.population
    # and for home value    
    country_hv_df = hv_df.groupby(['date'])[['pop_hv', 'population']].sum().reset_index()
    country_hv_df['home_value'] = country_hv_df.pop_hv/country_hv_df.population
    
    country_zdf = country_ri_df[['date', 'rent_index']].merge(country_hv_df[['date', 'home_value']], on=['date'])    

    return country_zdf, state_zdf, zdf


if __name__ == "__main__":
    country_zdf, state_zdf, zdf = get_zillow_data()
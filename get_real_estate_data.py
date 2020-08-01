# -*- coding: utf-8 -*-

import pandas as pd
import common as cmn

#def get_home_value_data():
zipfips = cmn.datapath/'ZipFips.csv'

zillow_home_value = 'http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv'
zillow_rental_index = 'http://files.zillowstatic.com/research/public_v2/zori/Zip_ZORI_AllHomesPlusMultifamily_SSA.csv'

zf = pd.read_csv(zipfips).set_index('ZIP')
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
zri_df.fips = zri_df.fips.apply(lambda x: "%05d"%x)
zri_df.date = pd.to_datetime(pd.to_datetime(zri_df.date) + pd.offsets.MonthEnd(0))

# clean and reformat home value data - remove unused columns and stack columns to rows
zhv_df['fips'] = zhv_df.StateCodeFIPS.apply(lambda x: "%02d"%x) + zhv_df.MunicipalCodeFIPS.apply(lambda x: "%03d"%x) 
zhv_df = zhv_df.drop(columns=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'State', 'Metro'])
zhv_df = zhv_df.drop(columns=[col for col in zhv_df.columns if not col.startswith('2020')])
zhv_df = zhv_df.stack().reset_index()
zhv_df.columns = ['fips', 'date', 'rent_index']
zhv_df.date = pd.to_datetime(zhv_df.date)

#    return zhv_df
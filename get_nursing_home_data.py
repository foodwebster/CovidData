# -*- coding: utf-8 -*-

import pandas as pd

def get_nursing_home_data():
    '''
    CDC nursing home data, described at:
    https://data.cms.gov/Special-Programs-Initiatives-COVID-19-Nursing-Home/COVID-19-Nursing-Home-Dataset/s2uc-8wxp
    '''
    url = 'https://data.cms.gov/api/views/s2uc-8wxp/rows.csv?accessType=DOWNLOAD'
    df = pd.read_csv(url)
    return df
    
if __name__ == "__main__":
    # execute only if run as a script
    df = get_nursing_home_data()
    
    facility_id = 'Federal Provider Number'
    resident_confirmed_cases = 'Residents Total Confirmed COVID-19'
    staff_CV19_deaths = 'Staff Total COVID-19 Deaths'
    resident_CV19_deaths = 'Residents Total COVID-19 Deaths'
    
    facility_grps = df.groupby(facility_id)
    n_fac_with_cases = (facility_grps[resident_confirmed_cases].max() > 0).sum()
    n_fac_with_deaths = (facility_grps[resident_CV19_deaths].max() > 0).sum()
    resident_deaths = facility_grps[resident_CV19_deaths].max().sum()    
    staff_deaths = facility_grps[staff_CV19_deaths].max().sum()

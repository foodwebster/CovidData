# -*- coding: utf-8 -*-

import pandas as pd

def get_hospital_data():
    # california covid hospital data
    ca_url = 'https://data.chhs.ca.gov/dataset/6882c390-b2d7-4b9a-aefa-2068cee63e47/resource/6cd8d424-dfaa-4bdd-9410-a3d656e1176e/download/covid19data.csv'
    
    url = 'https://www.cdc.gov/nhsn/pdfs/covid19/covid19-NatEst.csv'
    df = pd.read_csv(url)
    # line 2 is description of the columns
    df.drop(index=1)
    # date is ddMMMyyyy
    
    country_df = df[df.state == 'US']
    state_df = df[df.state != 'US']
    return state_df, country_df
    
if __name__ == "__main__":
    # execute only if run as a script
    state_df, country_df = get_hospital_data()
# -*- coding: utf-8 -*-

'''
https://www.countyhealthrankings.org/app/alabama/2020/downloads

https://www.countyhealthrankings.org/sites/default/files/media/document/2020%20County%20Health%20Rankings%20District%20of%20Columbia%20Data%20-%20v1_0.xlsx

https://www.countyhealthrankings.org/sites/default/files/media/document/2020%20County%20Health%20Rankings%20Alabama%20Data%20-%20v1_1.xlsx

https://www.countyhealthrankings.org/sites/default/files/media/document/2020%20County%20Health%20Rankings%20Alabama%20Data%20-%20v1_0.xlsx
'''

import pandas as pd

states = [
    'Alabama',
    'Alaska',
    'Arizona',
    'Arkansas',
    'California',
    'Colorado',
    'Connecticut',
    'Delaware',
    'District of Columbia',
    'Florida',
    'Georgia',
    'Hawaii',
    'Idaho',
    'Illinois',
    'Indiana',
    'Iowa',
    'Kansas',
    'Kentucky',
    'Louisiana',
    'Maine',
    'Maryland',
    'Massachusetts',
    'Michigan',
    'Minnesota',
    'Mississippi',
    'Missouri',
    'Montana',
    'Nebraska',
    'Nevada',
    'New Hampshire',
    'New Jersey',
    'New Mexico',
    'New York',
    'North Carolina',
    'North Dakota',
    'Ohio',
    'Oklahoma',
    'Oregon',
    'Pennsylvania',
    'Rhode Island',
    'South Carolina',
    'South Dakota',
    'Tennessee',
    'Texas',
    'Utah',
    'Vermont',
    'Virginia',
    'Washington',
    'West Virginia',
    'Wisconsin',
    'Wyoming'
]

versions = {
    'Alabama': '1',
    'North Dakota': '1'
}

def get_health_data_of_state(state):
    print("Getting data for %s"%state)
    state = state.replace(' ', '%20')
    version = '0' if state not in versions else versions[state]
    url = 'https://www.countyhealthrankings.org/sites/default/files/media/document/2020%20County%20Health%20Rankings%20' + \
        state + '%20Data%20-%20v1_' + version + '.xlsx'
    df = pd.read_excel(url)
    return df

state_dfs = {state: get_health_data_of_state(state) for state in states[34:]}


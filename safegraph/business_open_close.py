#####
#
# Business Count Script
# Author: Steve Scott
# Contact: sscott1@cityops.nyc.gov
#
# Counts existing businesses, businesses opening, and businesses cosing
# pulls from SafeGraph Core-POI dataset
#
# 
#####
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from typing import List, Dict
from utils import Utils
import numpy as np
from datetime import datetime


dotenv_path = Path( f'c:\\Users\\{os.getlogin()}\\secrets\\.env')
load_dotenv(dotenv_path)
ROOT = Path(os.getenv('MAYOR_DASHBOARD_ROOT'))

class BusinessOpenClose: 
    def __init__(self):
        self.utils : Utils = Utils()

    def main(self):
        business_df = self.make_business_open_close_df()
        business_df.to_csv(ROOT / 'output' / 'safegraph' / 'businesses_open_close_raw.csv', index=False)
        answer = self.find_counts(business_df)
        answer.to_csv(ROOT / 'output' / 'safegraph' / 'open_closed_counts.csv', index=False)
    
    def make_business_open_close_df(self):
        #for week in patterns
        print("business_count")
        poi_cbg = {}
        opened_on ={}
        closed_on={}
        naics_code={}
        i = 0
        for file_str in os.listdir(ROOT / 'safegraph' / 'csvs' / 'core_poi_weekly_patterns'):
            if i < 99999:
                #read month
                print(f"business open close: {file_str}")
                file_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'core_poi_weekly_patterns' / file_str, sep='|', dtype='object')
                #filter by geography as new month
                filter_file = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'filter_files' / 'nyc_counties.csv', dtype={'poi_cbg': str} )
                filtered = self.utils.filter_poi_to_region(file_df, filter_file )
                #place poicbg in dict with placekey as key
                poi_cbg.update(self.make_dict(filtered, 'poi_cbg'))
                opened_on.update(self.make_dict(filtered, 'opened_on'))
                #filtered does not contain closed_on because poi_cbg and naics_code are null for closed businesses.
                #core-poi is null when a business is closed, but the patterns data will remain
                #the weekly patterns raw data in a join of core_poi and weekly_patterns data, joined by SafeGraph.
                closed_on.update(self.make_dict(file_df, 'closed_on'))
                naics_code.update(self.make_dict(file_df, 'naics_code'))
                i += 1
        #create a dataframe based on keys.
        #calculate opens and closes
        all_businesses : pd.DataFrame = pd.DataFrame(columns=['placekey', 'poi_cbg', 'naics_code', 'opened_on', 'closed_on'], dtype='object')
        key_list = list(opened_on.keys())
        list_of_rows = []
        for _key in key_list:
            row = {
                'placekey': _key,
                'poi_cbg': self.val_or_default(_key, poi_cbg, '999999999'),
                'naics_code': self.val_or_default(_key, naics_code, '99999'),
                'opened_on': self.val_or_default(_key, opened_on, '9999-01'),
                'closed_on': self.val_or_default(_key, closed_on, '9999-01'),
            }
            list_of_rows.append(row)
        list_of_rows_df = pd.DataFrame(list_of_rows, dtype='object')
        all_businesses = pd.concat([all_businesses, list_of_rows_df], ignore_index=True)
        print(all_businesses.head())
        print(all_businesses.info())
        return all_businesses    
        
    def find_counts(self, business_df) -> pd.DataFrame:
        months = pd.period_range(start='2018-01-01', end=str(datetime.now()), freq='M').to_list()
        dict_list = []
        for _month in months:
            str_month = str(_month)
            opened = len(business_df[business_df['opened_on'] == str_month])
            closed = len(business_df[business_df['closed_on'] == str_month])
            existing = len(business_df[(business_df['opened_on'] <= str_month) & (str_month < business_df['closed_on'])])
            answer_dict = {'month': str_month, 'opened': opened, 'closed': closed, 'existing': existing}
            #print(f"answer_dict: {answer_dict}")
            dict_list.append(answer_dict)
        return pd.DataFrame(dict_list)

    def make_dict(self, df, column):
        df = df.set_index('placekey')
        df = df[column]
        return df.to_dict()

    def val_or_default(self, key, dict_, default_val):
        try: return dict_[key]
        except KeyError:
            return default_val
'''
if __name__ == '__main__':
    business_open_close = BusinessOpenClose()
    business_open_close.main()
'''
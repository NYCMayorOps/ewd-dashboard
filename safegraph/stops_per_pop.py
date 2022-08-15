import csv
from multiprocessing.resource_sharer import stop
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from utils import Utils
load_dotenv(Path('c:\\\\Users\\sscott1\\secrets\\.env'))
ROOT = Path(os.getenv('MAYOR_DASHBOARD_ROOT'))
assert(ROOT) is not None
utils_ = Utils()

natterns = os.listdir(ROOT / 'safegraph' / 'csvs' / 'natterns')

class StopsPerPop:
    
    def __init__(self):
        pass
    
    def main(self):
        answer = pd.DataFrame({
                'month': pd.Series(dtype='str'),
                'stops_per_pop': pd.Series(dtype='float'),
                'stops_per_pop_pre_covid': pd.Series(dtype='float'),
                'percent_baseline': pd.Series(dtype='float')
            }
            )
        filter_file = pd.read_csv( ROOT/ 'safegraph' / 'csvs' / 'filter_files' / 'nyc_counties.csv', dtype={'poi_cbg': str}, sep=',' )
        for nattern in natterns:
            #filter out the old nattern pattern
            if len(nattern) < 25:
                continue
            '''
            get sum of all stops in nyc

            '''
            nattern_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns'/ nattern, sep='|', dtype={'area': str} )
            stops_per_pop_this_month = self.find_stops_per_pop(nattern, nattern_df, filter_file)
            this_month = utils_.get_date_from_nattern_name(nattern)
            pre_covid_date = utils_.pre_covid_date(this_month)
            pre_nattern_name=f'nattern_normalized_{pre_covid_date}.csv'
            pre_covid_nattern_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / pre_nattern_name, sep='|', dtype={'area': str} )
            stops_per_pop_pre_covid = self.find_stops_per_pop(pre_nattern_name, pre_covid_nattern_df, filter_file)
            percent_change = (stops_per_pop_this_month - stops_per_pop_pre_covid) / stops_per_pop_pre_covid
            this_row = {
                'month': this_month,
                'stops_per_pop': stops_per_pop_this_month,
                'stops_per_pop_pre_covid': stops_per_pop_pre_covid,
                'percent_baseline': percent_change
            }
            print(f'stops_per_pop: {this_month}, {stops_per_pop_this_month}')
            answer = answer.append(this_row, ignore_index=True)
        answer.to_csv(ROOT / 'output' / 'safegraph' / 'stops_per_person_per_month_all_dates.csv', index=None)

    def find_stops_per_pop(self, nattern_name: str, nattern_df: pd.DataFrame, filter_file):
            nattern_df = utils_.filter_nattern_to_region(nattern_df, filter_file)
            total_stops_normalized = nattern_df['stops_normalized'].sum()

            #get the population
            year_month = utils_.get_date_from_nattern_name(nattern_name)
            year = year_month[:4]
            nyc_pop = utils_.population_dict[year]

            #calculate 
            stops_per_capita_per_month : float = total_stops_normalized / nyc_pop
            return stops_per_capita_per_month






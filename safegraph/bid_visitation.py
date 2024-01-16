#######
# an program to find the number of visitors to BIDS and CBDs
# There are two shapefiles. A one-to-one file to look at all BIDs, and a many-to-one
#######
import os
from dotenv import load_dotenv
from pathlib import Path
from RdpBucket import RdpBucket
import pandas as pd
import numpy as np
import pandas.api.types as ptypes
from typing import List
#import geopandas as gpd
from utils import Utils

load_dotenv(Path('c:\\\\Users\\sscott1\\secrets\\.env'))
ROOT = Path(os.getenv('MAYOR_DASHBOARD_ROOT'))
assert(ROOT) is not None
utils_ = Utils()

class BidVisitation:
    def __init__(self):
        pass

    def main(self):
        ''' A method that saves a pandas dataframe.
        '''
        # syncs files with rdp safegraph bucket
        rdp_bucket = RdpBucket()
        rdp_bucket.download_natterns()
        
        # instead of reading the shapefile, read the pre-extracted CSV
        #business improvement districts
        bid_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', dtype={'geoid': str, 'bidid': str})
        #central business districts. bidid is not numeric. 
        #also technically not a bid but that is what the column is called because they both go through the process_month method.
        cbd_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'CBD_CBGs.csv', dtype={'geoid': str, 'bidid': str})
        # for each month:
        list_of_months: List[pd.DataFrame] = []
        list_of_months_cbg: List[pd.DataFrame] = []
        for month in os.listdir(ROOT / 'safegraph' / 'csvs' / 'natterns'):
            print(month)
            if len(month) < 25:
                continue
            old_and_new_agg = self.process_month(month, bid_df)
            list_of_months.append(old_and_new_agg)
            old_and_new_agg = self.process_month(month, cbd_df)
            list_of_months_cbg.append(old_and_new_agg)
            #list_of_months_cbg = pd.concat([list_of_months_cbg, old_and_new_agg], axis = 0)
        answer = pd.concat(list_of_months, axis=0)
        answer.to_csv(ROOT / 'output' / 'safegraph' / 'bid_visitation_all_dates.csv', sep=',', index=False)
        answer = pd.concat(list_of_months_cbg)
        answer.to_csv(ROOT / 'output' / 'safegraph' / 'cbd_visitation_all_dates.csv', sep=',', index=False)

    def extract_bid_visit_dataframe(self, bid_df: pd.DataFrame, nattern: pd.DataFrame ) -> pd.DataFrame:
        '''
        joins the nattern csv to the BID geodataframe. Aggregates to the BID level.

        Args: bid_df - a static CSV of citywide census blocks. Census blocks within the BIDs are flagged.
                        One to many.
              nattern - the dataframe that comes from the neighborhood pattern csv from SafeGraph. 
                        One file per month.

        Returns: a geodataframe with the bid identifer and the sum of the normalized visits.
        '''
        try:
            assert bid_df['geoid'].dtype == nattern['area'].dtype
        except AssertionError:
            raise AssertionError(f"bid_df geoid is a {bid_df['geoid'].dtype} and nattern area is a {nattern['area'].dtype}. They need to be the same")
        #print(f"nattern info{nattern.info()}")
        nattern['stops_normalized'] = nattern['stop_counts']
        nattern['unique_visitors_normalized'] = nattern['device_counts']
        nattern = nattern[['area', 'date_range_start', 'stops_normalized', 'unique_visitors_normalized']]
        answer: pd.DataFrame = pd.merge(nattern, bid_df, left_on='area', right_on='geoid', how='left')
        return answer

    def aggregate_by_bid(self, bid_joined_nattern: pd.DataFrame):
        joined_df = bid_joined_nattern.groupby(by=['bid', 'bidid']).agg(stops=('stops_normalized', 'sum'), 
                                                                        unique_visitors=('unique_visitors_normalized', 'sum'), 
                                                                        date_range_start=('date_range_start', 'max')).reset_index()
        return joined_df.sort_values('bid')

    def get_bid_change(self, old_agg: pd.DataFrame, new_agg:pd.DataFrame):
        '''
            finds the percent change from old and new
            param old_agg: {bid: str e.g. "125th Street Bid"
                            date_range_start: str e.g. '2018-01-01T00:00:00-05:00'
                            stops : float
                            unique_visitors: float
            }
            returns: pd.DataFrame
        '''
        joined = pd.merge(new_agg, old_agg, how='left', on='bid', suffixes=['_this', '_pre_covid'])
        joined['percent_recovered_stops'] = ((joined['stops_this'] - joined['stops_pre_covid']) / joined['stops_pre_covid']) * 100
        joined['percent_recovered_visitors'] = ((joined['unique_visitors_this'] - joined['unique_visitors_pre_covid']) / joined['unique_visitors_pre_covid']) * 100
        return joined

    def process_month(self, filename: str, bid_df: pd.DataFrame):
        this_date = utils_.get_date_from_nattern_name(filename)
        print(f"bid visitation: {this_date}")
        pre_covid_date = utils_.pre_covid_date(this_date)
        #aggregate by bid for this month
        nattern : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / filename, sep=',', dtype={'area': str})
        nattern_joined : pd.DataFrame = self.extract_bid_visit_dataframe(bid_df, nattern)
        bid_agg : pd.DataFrame = self.aggregate_by_bid(nattern_joined)
        #aggregate pre_pandemic months
        old_nattern : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / f'natterns_plus_msa_{pre_covid_date}-01.csv.zip', sep=',', dtype={'area': str})
        nattern_joined = self.extract_bid_visit_dataframe(bid_df, old_nattern)
        old_agg : pd.DataFrame = self.aggregate_by_bid(nattern_joined)
        old_and_new_agg = self.get_bid_change(old_agg, bid_agg)
        old_and_new_agg['year-month'] = this_date
        return old_and_new_agg
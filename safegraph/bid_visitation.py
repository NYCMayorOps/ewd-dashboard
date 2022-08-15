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
        bid_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', dtype={'geoid': 'string'})
        cbd_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'CBD_CBGs.csv', dtype={'geoid': 'string'})
        # for each month:
        list_of_months: List[pd.DataFrame] = []
        list_of_months_cbg: List[pd.DataFrame] = []
        for month in os.listdir(ROOT / 'safegraph' / 'csvs' / 'natterns'):
            if len(month) < 25:
                continue
            old_and_new_agg = self.process_month(month, bid_df)
            list_of_months.append(old_and_new_agg)
            old_and_new_agg = self.process_month(month, cbd_df)
            list_of_months_cbg.append(old_and_new_agg)
        answer = pd.concat(list_of_months)
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
        assert ptypes.is_string_dtype(bid_df['geoid'])
        assert ptypes.is_string_dtype(nattern['area'])
        nattern = nattern[['area', 'date_range_start', 'stops_normalized', 'unique_visitors_normalized']]
        answer: pd.DataFrame = pd.merge(nattern, bid_df, left_on='area', right_on='geoid', how='left')
        return answer

    def aggregate_by_bid(self, bid_joined_nattern: pd.DataFrame):
        joined_df = bid_joined_nattern.groupby(by=['bid'], as_index=False).agg(stops=('stops_normalized', np.sum), unique_visitors=('unique_visitors_normalized', np.sum), date_range_start=('date_range_start', np.max))
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
        print(this_date)
        pre_covid_date = utils_.pre_covid_date(this_date)
        #aggregate by bid for this month
        nattern : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / filename, sep='|', dtype={'area': str})
        nattern_joined : pd.DataFrame = self.extract_bid_visit_dataframe(bid_df, nattern)
        bid_agg : pd.DataFrame = self.aggregate_by_bid(nattern_joined)
        #aggregate pre_pandemic months
        old_nattern : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / f'nattern_normalized_{pre_covid_date}.csv', sep='|', dtype={'area': str})
        nattern_joined = self.extract_bid_visit_dataframe(bid_df, old_nattern)
        old_agg : pd.DataFrame = self.aggregate_by_bid(nattern_joined) 
        old_and_new_agg = self.get_bid_change(old_agg, bid_agg)
        old_and_new_agg['year-month'] = this_date
        return old_and_new_agg
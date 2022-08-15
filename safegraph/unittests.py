import unittest
import pandas as pd
from pathlib import Path
from distance_from_home import DistanceFromHome
from bid_visitation import BidVisitation
from stops_per_pop import StopsPerPop


ROOT = Path('\\\\CHGOLDFS\\operations\\DEV_Team\\MayorDashboard\\repo')


class TestClass(unittest.TestCase):

    ##### distance from home #####
    '''
    def test_get_date_from_nattern_name(self):
        dfh = DistanceFromHome()
        answer = dfh.get_date_from_nattern_name('nattern_normalized_2022-06.csv')
        assert answer == '2022-06'
    def test_pre_covid_date(self):
        dfh = DistanceFromHome()
        assert dfh.pre_covid_date('2022-03') == '2019-03'
        assert dfh.pre_covid_date('2022-02') == '2020-02'
    '''
    ##### BID Visitation #####
    '''
    def test_bid_extract_visit_df(self):
        bv = BidVisitation()
        bid_dataframe = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', sep=',', dtype={'geoid': 'string'})
        nattern = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / 'nattern_normalized_2018-01.csv', sep='|', dtype={'area': 'string'})
        df = bv.extract_bid_visit_dataframe(bid_df=bid_dataframe, nattern=nattern)
        assert df['geoid'].count() == 5976
        assert df['bid'].count() == 916

    def test_aggregate_bid(self):
        bv = BidVisitation()
        bid_dataframe = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', sep=',', dtype={'geoid': 'string'})
        nattern = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / 'nattern_normalized_2018-01.csv', sep='|', dtype={'area': 'string'})
        bid_joined_nattern = bv.extract_bid_visit_dataframe(bid_df=bid_dataframe, nattern=nattern)
        assert bid_joined_nattern is not None
        aggregated = bv.aggregate_by_bid(bid_joined_nattern)
        assert len(aggregated) == 75

    def test_process_month(self):
        bv = BidVisitation()
        bid_dataframe = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', sep=',', dtype={'geoid': 'string'})
        filename='nattern_normalized_2022-06.csv'
        df = bv.process_month(filename, bid_dataframe)
        print(df.head(10))
    
    def test_bid_visitation_main(self):
        bv = BidVisitation()
        bv.main()
    '''
    ##### Stops Per Pop #####

    def test_spp_main(self):
        spp = StopsPerPop()
        spp.main()
if __name__=='__main__':
    unittest.main()
        
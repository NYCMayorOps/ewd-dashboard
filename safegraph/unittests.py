import unittest
import pandas as pd
from pathlib import Path
from distance_from_home import DistanceFromHome
from bid_visitation import BidVisitation
from stops_per_pop import StopsPerPop
from utils import Utils
from business_open_close import BusinessOpenClose


ROOT = Path('\\\\CHGOLDFS\\operations\\DEV_Team\\MayorDashboard\\repo')


class TestClass(unittest.TestCase):

    ##### distance from home #####
    
    def test_get_date_from_nattern_name(self):
        dfh = DistanceFromHome()
        answer = dfh.get_date_from_nattern_name('natterns_plus_msa_2019-01-01.csv.zip')
        try:
            assert answer == '2019-01-01'
        except:
            raise AssertionError('get_date_from_nattern_name failed {}'.format(answer))
    def test_pre_covid_date(self):
        dfh = DistanceFromHome()
        assert dfh.pre_covid_date('2022-03-01') == '2019-03-01'
        assert dfh.pre_covid_date('2022-02-01') == '2020-02-01'
    
    def test_get_date_from_nattern_name_2023(self):
        dfh = DistanceFromHome()
        answer = dfh.get_date_from_nattern_name('natterns_plus_msa_2023-11-01.csv.zip')
        try:
            assert answer == '2023-11-01'
        except:
            raise AssertionError('get_date_from_nattern_name failed {}'.format(answer))
    ##### BID Visitation #####
    
    def test_bid_extract_visit_df(self):
        bv = BidVisitation()
        bid_dataframe = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', sep=',', dtype={'geoid': str})
        nattern = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / 'natterns_plus_msa_2019-01-01.csv.zip', sep=',', dtype={'area': str})
        df = bv.extract_bid_visit_dataframe(bid_df=bid_dataframe, nattern=nattern)
        print(f" geoid = {df['geoid'].count()}, bid = {df['bid'].count()}")
        assert df['geoid'].count() == 6029
        assert df['bid'].count() == 916

    def test_aggregate_bid(self):
        bv = BidVisitation()
        bid_dataframe = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', sep=',', dtype={'geoid': str})
        nattern = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / 'natterns_plus_msa_2019-01-01.csv.zip', sep=',', dtype={'area': str})
        bid_joined_nattern = bv.extract_bid_visit_dataframe(bid_df=bid_dataframe, nattern=nattern)
        assert bid_joined_nattern is not None
        aggregated = bv.aggregate_by_bid(bid_joined_nattern)
        assert len(aggregated) == 75

    def test_process_month(self):
        bv = BidVisitation()
        bid_dataframe = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv', sep=',', dtype={'geoid': str} )
        filename='natterns_plus_msa_2023-11-01.csv.zip'
        df = bv.process_month(filename, bid_dataframe)

    
    '''
    def test_bid_visitation_main(self):
        bv = BidVisitation()
        bv.main()
    '''
    ##### Stops Per Pop #####
    '''
    def test_spp_main(self):
        spp = StopsPerPop()
        spp.main()
    '''

    ##### utils #####
    def test_filter_poi_to_region(self):
        utils_ = Utils()
        latest_df : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'core_poi_weekly_patterns' / 'pplus_msa_2019-01-07.gz', sep=',', dtype={'poi_cbg' : str})
        # filter Core POI to NYC
        filter_file_path : Path = ROOT / 'safegraph' / 'csvs' / 'filter_files' / 'nyc_counties.csv'
        filter_file : pd.DataFrame = pd.read_csv(filter_file_path, dtype={'poi_cbg': str})
        answer = utils_.filter_poi_to_region(latest_df, filter_file)
        assert len(answer) > 1
        assert len(set(answer['county'])) == 5


    def test_filter_nattern_to_region(self):
        utils = Utils()
        latest_df : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / 'natterns_plus_msa_2019-01-01.csv.zip', sep=',', dtype={'area' : str})
        # filter Core POI to NYC
        filter_file_path : Path = ROOT / 'safegraph' / 'csvs' / 'filter_files' / 'nyc_counties.csv'
        filter_file : pd.DataFrame = pd.read_csv(filter_file_path, dtype={'poi_cbg': str})
        answer = utils.filter_nattern_to_region(latest_df, filter_file)
        assert len(answer) > 1
        assert len(set(answer['county'])) == 5

    ##### business opened and closed #####
    def test_business_make_dict(self):
        bc = BusinessOpenClose()
        df = pd.DataFrame({'placekey':['aa','bb','cc'], 'j':['a','b', 'c'], 'k':['1', '2', '3'] })
        answer = bc.make_dict(df, 'j' )
        print(answer)
        assert isinstance(answer, dict)
        assert len(answer) == 3
        assert answer['aa'] == 'a'
    '''
    #deprecated
    def test_find_counts(self):
        bc = BusinessOpenClose()
        df = pd.DataFrame({'placekey': ['a','b','c'], 'opened_on':['2018-02', '2018-03', '2018-04'], 'closed_on':['2019-01', '2019-02', '9999-01']})
        answer = bc.find_counts(df)
        head = answer.loc[answer['month'] == '2018-01'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 0
            assert row['closed'] == 0
            assert row['existing'] == 0

        head = answer.loc[answer['month'] == '2018-02'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 1
            assert row['closed'] == 0
            assert row['existing'] == 1

        head = answer.loc[answer['month'] == '2018-03'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 1
            assert row['closed'] == 0
            assert row['existing'] == 2

        head = answer.loc[answer['month'] == '2018-04'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 1
            assert row['closed'] == 0
            assert row['existing'] == 3

        head = answer.loc[answer['month'] == '2018-05'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 0
            assert row['closed'] == 0
            assert row['existing'] == 3

        head = answer.loc[answer['month'] == '2019-01'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 0
            assert row['closed'] == 1
            assert row['existing'] == 2

        head = answer.loc[answer['month'] == '2019-02'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 0
            assert row['closed'] == 1
            assert row['existing'] == 1

        head = answer.loc[answer['month'] == '2019-03'].head(1)
        for index, row, in head.iterrows():
            assert row['opened'] == 0
            assert row['closed'] == 0
            assert row['existing'] == 1
    '''

    def test_get_date_from_nattern_name_2023(self):
            dfh = DistanceFromHome()
            answer = dfh.get_date_from_nattern_name('natterns_plus_msa_2023-11-01.csv.zip')
            try:
                assert answer == '2023-11-01'
            except:
                raise AssertionError('get_date_from_nattern_name failed {}'.format(answer))
            
    def test_filter_nattern_to_region_2023_11(self):
        '''
        this month was failing
        '''
        utils = Utils()
        latest_df : pd.DataFrame = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / 'natterns_plus_msa_2023-11-01.csv.zip', sep=',', dtype={'area' : str})
        # filter Core POI to NYC
        filter_file_path : Path = ROOT / 'safegraph' / 'csvs' / 'filter_files' / 'nyc_counties.csv'
        filter_file : pd.DataFrame = pd.read_csv(filter_file_path, dtype={'poi_cbg': str})
        answer = utils.filter_nattern_to_region(latest_df, filter_file)
        assert len(answer) > 1
        assert len(set(answer['county'])) == 5
if __name__=='__main__':
    unittest.main()
        
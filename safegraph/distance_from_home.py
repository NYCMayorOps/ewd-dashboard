#####
#
# a script to find the distance_from_home
# Author: Steve Scott
# Email: sscott1@cityops.nyc.gov
#
#####
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

dotenv_path = Path( f'c:\\Users\\{os.getlogin()}\\secrets\\.env')
load_dotenv(dotenv_path=dotenv_path)

ROOT = Path(os.getenv('MAYOR_DASHBOARD_ROOT'))
class DistanceFromHome:
    #the class that calculates distance from home in the natterns file.
    def __init__(self):
        pass

    def filter_nattern_to_region(self, nattern : pd.DataFrame, filter_file : pd.DataFrame):
        '''
        filters nattern to the geography specified in filter file.
        params:
            - nattern (dataframe) - a raw nattern file from SafeGraph
            - filter_file - a list of counties to include. May be MSA or city.

        returns:
            nattern DataFrame filtered to region
        '''
        fips = filter_file
        #filters the HPS to the region
        nattern['county'] = nattern['area'].astype(str).apply(lambda x: x[:5])
        #make a vector of in or out
        filter_ : List[bool] = []
        assert (type(nattern['county'][0]) == type(fips['poi_cbg'][0]))
        fips_list = fips['poi_cbg'].to_list()
        for county in nattern['county']:
            #print(f"{len(county)},  {len(fips['poi_cbg'][0])}")
            if county in fips_list:
                filter_.append(True)
            else:
                filter_.append(False)
        nattern_filtered = nattern[filter_]
        nattern_filtered = nattern_filtered.reset_index()
        #print(f"len nattern_filtered: {len(nattern_filtered)}")
        return nattern_filtered

    def main(self):
        '''iterates through the months, aggregates distance from home citywide,
        and compares to pre-pandemic baseline
        
        final dataframe: 
            {
               "date" : str  YYYY-MM '2022-01'
               "distance_from_home" : float
               "distance_from_home_pre_covid" : float
               "distance_percent_change" : float * 100%
            }

        returns: True, but does save the file.            
        '''
        # for all months
        answer_df = pd.DataFrame(columns=['date', 'distance_from_home', 'distance_from_home_pre_covid', 'distance_percent_change'])
        CSV_PATH = ROOT / 'safegraph' / 'csvs' 
        natterns = os.listdir(CSV_PATH / 'natterns')
        for filename in natterns:
            # Open safegraph
            if 'nattern_normalized' not in filename:
                continue
            this_date = self.get_date_from_nattern_name(filename)
            print(f"distance from home {this_date}")
            #find distance from home for this month
            this_nattern = pd.read_csv(CSV_PATH / 'natterns' /  filename, dtype={}, sep='|')
            filter_file = pd.read_csv( CSV_PATH / 'filter_files' / 'nyc_counties.csv', dtype={'poi_cbg': str}, sep=',' )
            this_nattern = self.filter_nattern_to_region(this_nattern, filter_file)
            distance_from_home = this_nattern['distance_from_home'].mean()
            
            # find distance from home pre-pandemic
            
            old_date = self.pre_covid_date(this_date)
            pp_nattern = pd.read_csv(CSV_PATH / 'natterns' / f'nattern_normalized_{old_date}.csv', sep='|' )
            pp_nattern = self.filter_nattern_to_region(pp_nattern, filter_file)
            distance_from_home_pre_covid = pp_nattern['distance_from_home'].mean()

            percent_change = (distance_from_home - distance_from_home_pre_covid) * 100.0 / distance_from_home_pre_covid

            new_df = pd.DataFrame({
                                    'date': [this_date],
                                    'distance_from_home': [distance_from_home],
                                    'distance_from_home_pre_covid': [distance_from_home_pre_covid],
                                    'percent_change': [percent_change]
                                  })
            answer_df = answer_df.append(new_df, ignore_index=True)
        answer_df.to_csv(ROOT / 'output' / 'safegraph' / 'distance_from_home_all_dates.csv', index=False, sep=',')
        print("distance from home complete")
        return True

        # isolate NYC by county
        # select distance_from_home column
        # aggregate citywide
        #
        # Join pre and post pandemic
        # calculate percent recovered pre and post.

    def get_date_from_nattern_name(self, name: str) -> str:
        name = name.split('.')[0]
        return name[-7:]

    def pre_covid_date(self, date: str) -> str:
        this_datetime = datetime.strptime(date, "%Y-%m")
        if this_datetime.month == 1 or this_datetime.month == 2:   
            return f'2020-{str(this_datetime.month).zfill(2)}'
        else:
            return f'2019-{str(this_datetime.month).zfill(2)}'
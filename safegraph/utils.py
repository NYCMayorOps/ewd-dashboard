from datetime import datetime
import pandas as pd
from typing import List

class Utils:    
    
    
    def __init__(self):
        self.population_dict = {
            '2018': 8398748, 
            '2019': 8336817,
            '2020': 8772978,
            '2021': 8467513,
            '2022': 8467513,
            '2023': 8467513,
        }

    def get_date_from_nattern_name(self, name: str) -> str:
        name = name.split('.')[0]
        return name[-7:]

    def pre_covid_date(self, date: str) -> str:
        this_datetime = datetime.strptime(date, "%Y-%m")
        if this_datetime.month == 1 or this_datetime.month == 2:   
            return f'2020-{str(this_datetime.month).zfill(2)}'
        else:
            return f'2019-{str(this_datetime.month).zfill(2)}'

    def filter_nattern_to_region(self, nattern : pd.DataFrame, filter_file : pd.DataFrame) -> pd.DataFrame:
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
    
    def filter_poi_to_region(self, poi : pd.DataFrame, filter_file : pd.DataFrame) -> pd.DataFrame:
        '''
        filters  poi to the geography specified in filter file.
        params:
            - poi - a poi file from SafeGraph rdp bucket
            - filter_file - a list of counties to include. May be MSA or city.

        returns:
            poi DataFrame filtered to region
        '''
        fips = filter_file
        #filters the HPS to the region
        poi['county'] = poi['poi_cbg'].astype(str).apply(lambda x: x[:5])
        #make a vector of in or out
        filter_ : List[bool] = []
        fips_list = fips['poi_cbg'].to_list()
        for county in poi['county']:
            #print(f"{len(county)},  {len(fips['poi_cbg'][0])}")
            if county in fips_list:
                filter_.append(True)
            else:
                filter_.append(False)
        poi_filtered = poi[filter_]
        poi_filtered = poi_filtered.reset_index()
        #print(f"len nattern_filtered: {len(nattern_filtered)}")
        return poi_filtered


#######
# an program to find the number of visitors to BIDS and CBDs
# There are two shapefiles. A one-to-one file to look at all BIDs, and a many-to-one
#######
import os
from dotenv import load_dotenv
from pathlib import Path
from RdpBucket import RdpBucket
import pandas as pd
import geopandas as gpd

load_dotenv(Path('c:\\\\Users\\sscott1\\secrets\\.env'))
ROOT = Path(os.getenv('MAYOR_DASHBOARD_ROOT'))
assert(ROOT) is not None

def main():
    # syncs files with rdp safegraph bucket
    rdp_bucket = RdpBucket()
    rdp_bucket.download_natterns()

    
    # read shapefile as geodataframe
    #bid_gdf = gpd.read_file(ROOT / 'shapefiles' / 'NYC_CBG_Many.shp' )
    #answer_gdf = bid_gdf.copy()

    # instead of reading the shapefile, read the pre-extracted CSV
    bid_df = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'bid_visitation' / 'bids_cbg_one_to_many.csv')
    # for each month:
    for month in os.listdir(ROOT / 'safegraph' / 'csvs' / 'natterns'):
        nattern = pd.read_csv(ROOT / 'safegraph' / 'csvs' / 'natterns' / month)
        this_month_agg = extract_bid_visit_df(bid_df, nattern)
    #isolate_bid_visits(bid_gdf, this_month)
    # join BID shapefile to latest month on CBGs
    # aggregate geodataframe by BID

    # join bid shapefile to pre pandemic month on cbg as a geodataframe
    # isolate 
    # aggregate geodataframe by bid


    # 
    # isolate the normalized visitation columns
    # join pre and post pandemic
    # calculate percent recovered.
    # export csv to network drive.
    # export geodataframe to carto?
    # concatentate all bid visitation files into a master file.

    #then do the same for citywide.

    #tests 
    # assert there is a shapefile
    # assert there is safegraph data
    # raise an exception if a bid cbg is not in safegraph cbg.

def extract_bid_visit_df(bid_df: pd.DataFrame, nattern: pd.DataFrame ) -> gpd.GeoDataFrame:
    '''
    joins the nattern csv to the BID geodataframe. Aggregates to the BID level.

    Args: bid_gdf - a static CSV of citywide census blocks. Census blocks within the BIDs are flagged.
                    One to many.
          nattern - the dataframe that comes from the neighborhood pattern csv from SafeGraph. 
                    One file per month.

    Returns: a geodataframe with the bid identifer and the sum of the normalized visits.
    '''
    nattern = nattern[['area', 'date_range_start', 'raw_stop_counts', 'raw_device_counts']]
    print(nattern.info())
    raise Exception("stop here")
    return None

if __name__=='__main__':
    main()
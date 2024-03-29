import os, sys
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import numpy as np
from carto.exceptions import CartoException
from cartoframes import to_carto
from cartoframes.auth import set_default_credentials
import geopandas as gpd
from iswindows import IsWindows

if IsWindows().is_windows:
    dotenv_path = Path( f'c:\\Users\\{os.getlogin()}\\secrets\\.env')
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()
    ###this should commit

api_key = os.getenv('CARTO_KEY')
username =os.getenv('CARTO_USERNAME')
zip_codes_path = os.getenv('ZIP_CODES_PATH')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
ROOT=os.getenv('MAYOR_DASHBOARD_ROOT')
sys.path.insert(0, ROOT + '/utils')
from sharepoint import Sharepoint

def main():
    mastercard_historic = os.listdir('./historic')
    mastercard_latest = sorted(mastercard_historic, reverse=True)[0]

    print(mastercard_latest)
    df = pd.read_csv(Path(ROOT) / 'mastercard' / 'historic' / f"{mastercard_latest}")
    #print(df.info())
    mastercard_old = pd.read_csv(Path(ROOT) / 'mastercard' / 'historic' / 'mastercard_2019-01-01_to_2021-04-25.csv')
    print('transforming new dates')
    df = mastercard_transform(df)
    #the date range is the same for all values. Take the first.
    min_date = df.date_range.apply(lambda x: x.split(' ')[0])[0]
    max_date = df.date_range.apply(lambda x: x.split(' ')[2])[0]
    min_date = pd.to_datetime(min_date)
    max_date = pd.to_datetime(max_date)
    print('transforming old dates')
    old_df = mastercard_transform_old_dates(mastercard_old, min_date, max_date)
    print('joining new and old dates')
    df = join_old_new_zip(old_df, df)
    authenticate_carto(api_key, username)
    upload_df_to_carto(df, 'mastercard latest')
    zip_codes = gpd.read_file(zip_codes_path)
    gdf = join_to_zipcodes(df, zip_codes)
    upload_df_to_carto(gdf, 'mastercard_latest_geo')
    #citywide transform now handled in mastercard_backfill
    #df = mastercard_transform_citywide_daily(mastercard_raw)
    #df = mastercard_transform_citywide_daily(mastercard_old)
    #print("writing mastercard_latest_citywide.csv")
    #df.to_csv(Path(os.getenv('MAYOR_DASHBOARD_ROOT')) / 'output' / 'mastercard' / 'mastercard_latest_citywide.csv')
    
    

def upload_df_to_carto(df, table_name):
    print('uploading to carto')
    to_carto(df, table_name, if_exists='replace')

def authenticate_carto(api_key, username):
    try:
        set_default_credentials(
            username=username,
            api_key=api_key
            )
    except CartoException as e:
        print("could not connect to Carto", e)

def mastercard_transform(df):
    df['date_range'] = f"{df.txn_date.min()} to {df.txn_date.max()}"
    df['zip_code'] = df['Zip_code'].astype(str)
    df = df.loc[df['segment'] == 'Overall'] 
    df = df.loc[df['industry'] == 'Total Retail']
    df = df.loc[df['geo_type'] == 'Msa']

    df_agg = df.groupby(['zip_code', 'date_range']).agg(txn_amt_index=('txn_amt_index', "mean"),
                                          txn_cnt_index=('txn_cnt_index', np.mean),
                                          avg_spend_amt_index=('avg_spend_amt_index', "mean"),
                                          #yoy_txn_amt=('yoy_txn_amt', np.mean),
                                          #yoy_txn_cnt=('yoy_txn_cnt', np.mean)    
                                        )

    #yr,txn_date,industry,segment,geo_type,geo_name,nation_name,zip_code,txn_amt_index,txn_cnt_index,acct_cnt_index,avg_ticket_index,avg_freq_index,avg_spend_amt_index,yoy_txn_amt,yoy_txn_cnt,borough,borocode
    df_agg.reset_index( inplace=True)
    #print('writing df_agg.csv')
    #df_agg.to_csv('df_agg.csv')
    return df_agg

def mastercard_transform_old_dates(df, min_date, max_date):
    '''
    input:
        df: the old mastercard dateframe from 2019-2020
    side-effects:
        this function writes to df_agg_old.csv
    '''
    min_month = min_date.strftime('%m')
    min_day = min_date.strftime('%d')
    max_month = max_date.strftime('%m')
    max_day = max_date.strftime('%d')
    print(f"dates before: {min_date}, {max_date}")
    #this is pre-pandemic
    if min_date.month == 12 and max_date.month == 1:
        min_date = f'2019-{min_month}-{min_day}'
        max_date = f'2020-{max_month}-{max_day}'
    elif min_date.month > 2 :
        min_date = f'2019-{min_month}-{min_day}'
        max_date = f'2019-{max_month}-{max_day}'
    else:
        min_date = f'2020-{min_month}-{min_day}'
        max_date = f'2020-{max_month}-{max_day}'
    print(f"min date after: {min_date}, max_date: {max_date}")
    assert (min_date < max_date)
    df = df.loc[(df.txn_date >= min_date) & (df.txn_date <= max_date)]
    assert(len(df) > 0)
    df = df.copy()
    df['date_range'] = f"{min_date} to {max_date}"
    df['zip_code'] = df['zip_code'].astype(str)
    df = df.loc[df['segment'] == 'Overall'] 
    df = df.loc[df['industry'] == 'Total Retail']
    df = df.loc[df['geo_type'] == 'Msa']
    #df.to_csv('check_old_dates.csv')

    df_agg = df.groupby(['zip_code', 'date_range']).agg(txn_amt_index=('txn_amt_index', "mean"),
                                          txn_cnt_index=('txn_cnt_index', "mean"),
                                          avg_spend_amt_index=('avg_spend_amt_index', "mean"),
                                          #these are coming in as strings.
                                          #yoy_txn_amt=('yoy_txn_amt', np.mean),
                                          #yoy_txn_cnt=('yoy_txn_cnt', np.mean)    
                                        )

    #yr,txn_date,industry,segment,geo_type,geo_name,nation_name,zip_code,txn_amt_index,txn_cnt_index,acct_cnt_index,avg_ticket_index,avg_freq_index,avg_spend_amt_index,yoy_txn_amt,yoy_txn_cnt,borough,borocode
    df_agg.reset_index( inplace=True)
    print('writing df_agg_old.csv')
    df_agg.to_csv('df_agg_old.csv')
    return df_agg



def join_old_new_zip(old_df, new_df):
    df = new_df.merge(old_df, on='zip_code', suffixes=('_post', '_pre'), how='outer')
    df['retail_recovery_nominal']= df['txn_amt_index_post'] - df['txn_amt_index_pre']
    df['retail_%_recovery'] = df['txn_amt_index_post'] / df['txn_amt_index_pre'] * 100
    return df

def join_to_zipcodes(df, zipcodes):
    df['ZIPCODE'] = df['zip_code']
    gdf_joined = zipcodes.merge(df, on='ZIPCODE', how='outer')
    return gdf_joined


#main is called by mastercard_main.py    
'''
if __name__ == '__main__':
    main()
'''
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import numpy as np
import carto
from carto.sql import SQLClient
from carto.exceptions import CartoException
from carto.auth import APIKeyAuthClient
from cartoframes import to_carto
from cartoframes.auth import set_default_credentials
import geopandas as gpd

dotenv_path = Path( 'c:\\Users\\sscott1\\secrets\\.env')
load_dotenv(dotenv_path=dotenv_path)

api_key = os.getenv('CARTO_KEY')
username =os.getenv('CARTO_USERNAME')
zip_codes_path = os.getenv('ZIP_CODES_PATH')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

def main():
    mastercard_raw = pd.read_csv('mastercard_latest.csv')
    #df = download_mastercard_latest()
    authenticate_carto(api_key, username)
    df = mastercard_transform(mastercard_raw)
    upload_df_to_carto(df, 'mastercard latest')
    zip_codes = gpd.read_file(zip_codes_path)
    gdf = join_to_zipcodes(df, zip_codes)
    upload_df_to_carto(gdf, 'mastercard_latest_geo')
    df = mastercard_transform_citywide_daily(mastercard_raw)
    print("writing mastercard_latest.csv")
    df.to_csv(Path(os.getenv('MAYOR_DASHBOARD_ROOT')) / 'output' / 'mastercard' / 'mastercard_latest.csv')

    

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
    df['zip_code'] = df['zip_code'].astype(str)
    df = df.loc[df['segment'] == 'Overall'] 
    df = df.loc[df['industry'] == 'Total Retail']
    df = df.loc[df['geo_type'] == 'Msa']

    df_agg = df.groupby(['zip_code', 'date_range']).agg(txn_amt_index=('txn_amt_index', np.mean),
                                          txn_cnt_index=('txn_cnt_index', np.mean),
                                          avg_spend_amt_index=('avg_spend_amt_index', np.mean),
                                          yoy_txn_amt=('yoy_txn_amt', np.mean),
                                          yoy_txn_cnt=('yoy_txn_cnt', np.mean)    
                                        )

    #yr,txn_date,industry,segment,geo_type,geo_name,nation_name,zip_code,txn_amt_index,txn_cnt_index,acct_cnt_index,avg_ticket_index,avg_freq_index,avg_spend_amt_index,yoy_txn_amt,yoy_txn_cnt,borough,borocode
    df_agg.reset_index( inplace=True)
    print('writing df_agg.csv')
    df_agg.to_csv('df_agg.csv')
    return df_agg

def mastercard_transform_citywide_daily(df):
    df['zip_code'] = df['zip_code'].astype(str)
    df = df.loc[df['segment'] == 'Overall'] 
    df = df.loc[df['industry'] == 'Total Retail']
    df = df.loc[df['geo_type'] == 'Msa']

    df_agg = df.groupby(['txn_date']).agg(txn_amt_index=('txn_amt_index', np.mean),
                                          txn_cnt_index=('txn_cnt_index', np.mean),
                                          avg_spend_amt_index=('avg_spend_amt_index', np.mean),
                                          yoy_txn_amt=('yoy_txn_amt', np.mean),
                                          yoy_txn_cnt=('yoy_txn_cnt', np.mean)    
                                        )

    #yr,txn_date,industry,segment,geo_type,geo_name,nation_name,zip_code,txn_amt_index,txn_cnt_index,acct_cnt_index,avg_ticket_index,avg_freq_index,avg_spend_amt_index,yoy_txn_amt,yoy_txn_cnt,borough,borocode
    df_agg.reset_index( inplace=True)
    print('writing df_agg.csv')
    return df_agg

def join_to_zipcodes(df, zipcodes):
    df['ZIPCODE'] = df['zip_code']
    gdf_joined = zipcodes.merge(df, on='ZIPCODE')
    return gdf_joined

def download_mastercard_latest():

    return None
    

if __name__ == '__main__':
    main()
import os, sys
import pandas as pd
from pathlib import Path
import mastercard_upload as mcu
from dotenv import load_dotenv
import numpy as np

from iswindows import IsWindows

if IsWindows().is_windows:
    dotenv_path = Path( f'c:\\Users\{os.getlogin()}\\secrets\\.env')
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()
ROOT = os.getenv('MAYOR_DASHBOARD_ROOT')
OUTPUT_DIR = Path(os.getenv('OUTPUT_DIR'))
sys.path.insert(0, ROOT + '/utils')
#from sharepoint import Sharepoint
#from iswindows import IsWindows



def main():
    print("reading historic data")
    
    #mastercard_raw = pd.read_csv('historic_daily.csv')
    
    #pre-baked. Uncomment to re-run
    mastercard_raw = concat_all_files(Path(ROOT))
    mastercard_raw.to_csv('historic_daily.csv', index=False)
    print("transforming old data")
    mastercard_old = pd.read_csv(Path(os.getcwd()) / 'historic' / 'mastercard_2019-01-01_to_2021-04-25.csv')
    df_new = mastercard_transform_citywide_daily(mastercard_raw)
    mastercard_raw = None
    df_old = mastercard_transform_citywide_daily(mastercard_old)
    #mastercard_old = None
    print("joining old to new")
    df = join_old_new_zip(df_old, df_new)
    my_path = OUTPUT_DIR / 'mastercard_all_dates_citywide.csv'
    print(f"writing to {my_path}")
    df.to_csv(my_path, index=False)
    #print(df.info())
    '''
    #df = pd.read_csv('df_agg.csv')
    #my_path = OUTPUT_DIR / 'mastercard_all_dates_citywide.csv'
    #sp = Sharepoint()
    #sp.upload_file( my_path, 'Mastercard', 'test_file.csv', sp.ops)
    '''
    




def concat_all_files(root):
    files = os.listdir(root / 'mastercard' /'historic')
    files = sorted(files)
    file_paths = [root / 'mastercard' / 'historic' / file for file in files]
    print("reading csvs")
    file_frames = [pd.read_csv(x) for x in file_paths]
    print("concatenating csvs into one large file")
    df = pd.concat(file_frames)
    #print(df.info())
    #raise Exception('stop-here')
    df.to_csv('historic_daily.csv', index=False)
    return df

def mastercard_transform_citywide_daily(df):
    df['zip_code'] = df['zip_code'].astype(str)
    df = df.loc[df['segment'] == 'Overall'] 
    df = df.loc[df['industry'] == 'Total Retail']
    df = df.loc[df['geo_type'] == 'Msa']

    df_agg = df.groupby(['txn_date']).agg(txn_amt_index=('txn_amt_index', np.mean),
                                          txn_cnt_index=('txn_cnt_index', np.mean),
                                          avg_spend_amt_index=('avg_spend_amt_index', np.mean),
                                          #yoy_txn_amt=('yoy_txn_amt', np.mean),
                                          #yoy_txn_cnt=('yoy_txn_cnt', np.mean)    
                                        )

    #yr,txn_date,industry,segment,geo_type,geo_name,nation_name,zip_code,txn_amt_index,txn_cnt_index,acct_cnt_index,avg_ticket_index,avg_freq_index,avg_spend_amt_index,yoy_txn_amt,yoy_txn_cnt,borough,borocode
    df_agg.reset_index( inplace=True)
    #print('writing df_agg.csv')
    return df_agg

def join_old_new_zip(old_df, new_df):

    new_df_copy = new_df.copy()
    for index, row in new_df_copy.iterrows():
        parsed_date = pd.to_datetime(row['txn_date'])
        my_month = parsed_date.strftime('%m')
        my_day = parsed_date.strftime('%d')
        if parsed_date.month > 2 :
            my_date = f'2019-{my_month}-{my_day}'
        else:
            my_date = f'2020-{my_month}-{my_day}'
        new_df.at[index, 'txn_date_old'] = my_date

    df = new_df.merge(old_df, left_on='txn_date_old', right_on='txn_date', suffixes=('_post', '_pre'), how='outer')
    df['retail_recovery_nominal']= df['txn_amt_index_post'] - df['txn_amt_index_pre']
    df['retail_%_recovery'] = df['txn_amt_index_post'] / df['txn_amt_index_pre'] * 100
    return df

#main is called externally by mastercard_main.py
#upload_file(self, local_path, rel_path, filename, ctx):
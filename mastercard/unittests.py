import unittest
from mastercard_upload import mastercard_transform_old_dates
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os
load_dotenv(Path('c:') / os.getlogin() / 'secrets' / '.env')
ROOT = os.getenv('MAYOR_DASHBOARD_ROOT')

class TestClass(unittest.TestCase):
    test_df = pd.DataFrame({'txn_date': ['2022-12-25', '2022-12-31', '2023-01-03']})
    mastercard_old = pd.read_csv(Path(ROOT) / 'mastercard' / 'historic' / 'mastercard_2019-01-01_to_2021-04-25.csv')
    mastercard_transform_old_dates(mastercard_old, pd.to_datetime('2022-12-26'), pd.to_datetime('2023-01-03'))

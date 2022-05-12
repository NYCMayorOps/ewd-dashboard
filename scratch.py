import os 
from utils.sharepoint import Sharepoint
from pathlib import Path
sp = Sharepoint()

sp.upload_file(Path(os.getcwd()) / 'mastercard' / 'mastercard_latest.csv', 'Documents/Mastercard', 'mastercard_latest_test.csv', sp.ops )

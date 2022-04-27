import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path( 'c:\\Users\\sscott1\\secrets\\.env')
load_dotenv(dotenv_path=dotenv_path)

key = os.getenv('CARTO_KEY')


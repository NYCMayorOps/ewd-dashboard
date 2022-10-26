import os
import boto3
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(f'c:\\Users\\{os.getlogin()}\\secrets\.env' ))

class RdpBucket: 
    #a class for the rdp connection
    def __init__(self):
        self.s3 = self.connect_to_rdp_s3()

    def connect_to_rdp_s3(self) -> boto3.Session:
        """
        Connects to s3 and creates a session
        Args:
            none
        
        returns:
            boto3.Session
        """
        RDP_AWS_ACCESS_KEY_ID = os.getenv('RDP_AWS_ACCESS_KEY_ID')
        RDP_AWS_SECRET_ACCESS_KEY = os.getenv('RDP_AWS_SECRET_ACCESS_KEY')
    
        session = boto3.Session(aws_access_key_id=RDP_AWS_ACCESS_KEY_ID, aws_secret_access_key=RDP_AWS_SECRET_ACCESS_KEY)
        s3 = session.resource('s3')
        print(type(s3))
        return s3

    def download_natterns(self) -> None :
        """
        Args:
        
        Returns:
            None

        Raises:
        """
        ROOT = os.getenv('MAYOR_DASHBOARD_ROOT')
        assert ROOT is not None
        s3 = self.s3
        bucket_name='safegraph-post-rdp'
        prefix='output/neighborhood_patterns/nattern'
        my_bucket = s3.Bucket(bucket_name)
        keys = []
        for object in my_bucket.objects.filter(Prefix=prefix):
            keys.append(object.key)
        sg_historic_path = Path(ROOT) / 'safegraph' / 'csvs' / 'natterns' # nattern is short for "neighborhood pattern"
        old_historic = os.listdir(sg_historic_path)
        #print(f"old_historic: {old_historic}")
        #print(f"keys: {keys}")
        for key in keys:
            obj_name = key.split('/')[-1]
            if obj_name not in old_historic:
                print(f"downloading {obj_name}")
                filename =  (sg_historic_path / obj_name).as_posix()
                my_bucket.download_file(f'{key}', f'{filename}')
        print("All natterns are up to date.")

    def download_core_poi(self):
        ROOT = os.getenv('MAYOR_DASHBOARD_ROOT')
        s3 = self.s3
        bucket_name ='safegraph-post-rdp'
        prefix = 'output/weekly_patterns/weekly_pattern'
        my_bucket = s3.Bucket(bucket_name)
        keys = []
        for object in my_bucket.objects.filter(Prefix=prefix):
            keys.append(object.key)
        poi_historic_path = Path(ROOT) / 'safegraph' / 'csvs' / 'core_poi_weekly_patterns'
        old_historic = os.listdir(poi_historic_path)
        for key in keys:
            obj_name = key.split('/')[-1]
            if obj_name not in old_historic:
                print(f"downloading {obj_name}")
                filename = (poi_historic_path / obj_name).as_posix()
                my_bucket.download_file(f'{key}', f'{filename}')
        print('All Core POI Weekly Patterns up to date')
'''
    def download_nattern_home_pannel_summaries(self) -> None :
        """
        Args:
        
        Returns:
            None

        Raises:
        """
        ROOT = os.getenv('MAYOR_DASHBOARD_ROOT')
        s3 = self.s3
        bucket_name='safegraph-post-rdp'
        prefix='output/neighborhood_patterns/nattern'
        my_bucket = s3.Bucket(bucket_name)
        keys = []
        for object in my_bucket.objects.filter(Prefix=prefix):
            keys.append(object.key)
        sg_historic_path = Path(ROOT) / 'safegraph' / 'csvs' / 'natterns' # nattern is short for "neighborhood pattern"
        old_historic = os.listdir(sg_historic_path)
        #print(f"old_historic: {old_historic}")
        #print(f"keys: {keys}")
        for key in keys:
            obj_name = key.split('/')[-1]
            if obj_name not in old_historic:
                print(f"downloading {obj_name}")
                filename =  (sg_historic_path / obj_name).as_posix()
                my_bucket.download_file(f'{key}', f'{filename}')
        print("All natterns are up to date.")
'''
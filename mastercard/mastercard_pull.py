import os
import boto3
from dotenv import load_dotenv
from pathlib import Path
from iswindows import IsWindows

if IsWindows().is_windows:
    dotenv_path = Path( 'c:\\Users\\sscott1\\secrets\\.env')
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()


class Pull:
    def connect_to_rdp_s3(self):
        RDP_AWS_ACCESS_KEY_ID = os.getenv('RDP_AWS_ACCESS_KEY_ID')
        RDP_AWS_SECRET_ACCESS_KEY = os.getenv('RDP_AWS_SECRET_ACCESS_KEY')
    
        session = boto3.Session(aws_access_key_id=RDP_AWS_ACCESS_KEY_ID, aws_secret_access_key=RDP_AWS_SECRET_ACCESS_KEY)
        s3 = session.resource('s3')
        print(type(s3))
        return s3


    def pull(self):
        ROOT = os.getenv('MAYOR_DASHBOARD_ROOT')
        s3 = self.connect_to_rdp_s3()
        bucket_name='recovery-data-partnership'
        prefix='mastercard_processed/mastercard_20'
        my_bucket = s3.Bucket(bucket_name)
        keys = []
        for object in my_bucket.objects.filter(Prefix=prefix):
            keys.append(object.key)
        mc_historic_path = Path(ROOT) / 'mastercard' / 'historic' 
        old_historic = os.listdir(mc_historic_path)
        #print(f"old_historic: {old_historic}")
        #print(f"keys: {keys}")
        for key in keys:
            obj_name = key.split('/')[-1]
            if obj_name not in old_historic:
                obj_name2 = obj_name.split('.')[0]
                print(f"downloading {obj_name}")
                filename =  (mc_historic_path / obj_name).as_posix()
                my_bucket.download_file(f'{key}', f'{filename}')

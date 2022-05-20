from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
import sys
import os
import glob
from io import StringIO, BytesIO
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from iswindows import IsWindows

if IsWindows().is_windows:
    dotenv_path = Path( 'c:\\Users\\sscott1\\secrets\\.env')
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()
class Sharepoint:
 
    def __init__(self):
        self.ops = self.connect_ops()
        self.rdp = self.connect_rdp()
        print(self.test_ctx(self.ops))
        print(self.test_ctx(self.rdp))
    '''
    def mkdir_recursive(self, root_folder, path):
        # Similar to mkdir -p <path>
        stems = path.split('/')
        relative_folder = stems.pop(0)
        self.mkdir(root_folder, relative_folder)
        new_root_folder=f'{root_folder}/{relative_folder}'
        new_path='/'.join(stems)
        if len(stems) >= 1:
            self.mkdir_recursive(new_root_folder, new_path)
    
    def mkdir(self, root_folder:str, relative_folder:str, ctx):
        # Similar to mkdir <path>
        root = ctx.web.get_folder_by_server_relative_url(root_folder)
        relative = root.folders.add(relative_folder)
        ctx.execute_query()
        print(f'new folder: {root_folder}/{relative_folder}')

    def copy_file(self, local_path, target_path, ctx):
        # similar to `cp file.txt <sharepoint.com>/root/path/to/file.txt` 
        root = target_path.split('/')[0]
        parent = '/'.join(target_path.split('/')[1:-1])
        relative = '/'.join(target_path.split('/')[0:-1])
        file_name = target_path.split('/')[-1]
        self.mkdir_recursive(root, parent)

        # Load in the file to file_content
        with open(local_path, 'rb') as content_file:
            file_content = content_file.read()

        #test for empty df if the data is a CSV (may be other types like shapefile or txt)
        if file_name.split('.')[-1] == 'csv':
            df = pd.read_csv(BytesIO(file_content), nrows=10)
            if df.empty == True:
                raise Exception("The CSV was empty")
            else:
                print("CSV is valid")


        # Upload file to target path
        target_folder = ctx.web.get_folder_by_server_relative_url(relative)
        target_file = target_folder.upload_file(file_name, file_content)
        ctx.execute_query()
        print(f'Copied {local_path} to {target_path}')
    '''
    def upload_file(self, local_path, rel_path, filename, ctx):
        #root = target_path.split('/')[0]
        #parent = '/'.join(target_path.split('/')[1:-1])
        #relative = '/'.join(target_path.split('/')[0:-1])
        #file_name = target_path.split('/')[-1]
        local_filename = str(local_path).split('/')[-1]
        with open(local_path, 'rb') as content_file:
            file_content = content_file.read()
        if local_filename.split('.')[-1] == 'csv':
            df = pd.read_csv(BytesIO(file_content), nrows=10)
            if df.empty == True:
                raise Exception("The CSV was empty")
            else:
                print("CSV is valid")
        # Upload file to target path
        target_folder = ctx.web.get_folder_by_server_relative_url(rel_path)
        print(f"target_folder = {target_folder}")
        target_file = target_folder.upload_file(filename, file_content)
        ctx.execute_query()
        print(f'Copied {local_path} to {rel_path}')


    def connect_rdp(self):
        settings=dict(
            url=os.environ['RDP_SHAREPOINT_URL'],
            client_credentials=dict(
                client_id=os.environ.get('RDP_SHAREPOINT_CLIENT_ID', ''),
                client_secret=os.environ.get('RDP_SHAREPOINT_CLIENT_SECRET', '')
            )
        )

        ctx = ClientContext(settings['url']).with_credentials(
        ClientCredential(settings['client_credentials']['client_id'],
                         settings['client_credentials']['client_secret']))
        return ctx

    def connect_ops(self):
        settings=dict(
            url=os.environ['OPS_SHAREPOINT_URL'],
            client_credentials=dict(
                client_id=os.environ.get('OPS_SHAREPOINT_CLIENT_ID', ''),
                client_secret=os.environ.get('OPS_SHAREPOINT_CLIENT_SECRET', '')
            )
        )

        ctx = ClientContext(settings['url']).with_credentials(
        ClientCredential(settings['client_credentials']['client_id'],
                         settings['client_credentials']['client_secret']))
        return ctx

    def test_ctx(self, ctx):
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print("Web title: {0}".format(web.properties['Title']))
        return True

    '''
        target_folder=sys.argv[1]
        for local_path in glob.glob("output/*"):
            file_name=local_path.split('/')[-1]
            target_path=f'{target_folder}/{file_name}'
            copy_file(local_path, target_path)
    '''
    
if __name__ == '__main__':
    sp = Sharepoint()
    ctx = sp.ops
    my_path = Path(os.getenv('OUTPUT_DIR')) / 'mastercard_all_dates_citywide.csv'
    sp = Sharepoint()
    sp.upload_file( my_path, 'Mastercard', 'mastercard_all_dates_citywide.csv', ctx)

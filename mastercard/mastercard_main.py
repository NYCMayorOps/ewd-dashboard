import mastercard_upload
import mastercard_backfill
from mastercard_pull import Pull

if __name__ == '__main__':
    Pull().pull()
    mastercard_upload.main()
    mastercard_backfill.main()
    print('mastercard main complete')
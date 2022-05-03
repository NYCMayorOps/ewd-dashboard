import mastercard_upload
import mastercard_backfill

if __name__ == '__main__':
    mastercard_upload.main()
    mastercard_backfill.main()
    print('mastercard main complete')
from RdpBucket import RdpBucket

def main():
    synchronize()

def synchronize():
    rdp_bucket = RdpBucket()
    rdp_bucket.download_natterns()
    rdp_bucket.download_core_poi()

if __name__=='__main__':
    main()
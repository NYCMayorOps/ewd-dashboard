from business_open_close import BusinessOpenClose
from distance_from_home import DistanceFromHome
from bid_visitation import BidVisitation
from stops_per_pop import StopsPerPop
from business_open_close import BusinessOpenClose
from synchronize import synchronize

synchronize()

print('BID vistitation')
bv = BidVisitation()
bv.main()

print('distance from home')
dfh = DistanceFromHome()
dfh.main()

print('stops per pop')
spp = StopsPerPop()
spp.main()

'''
print('business open close')
boc = BusinessOpenClose()
boc.main()
'''


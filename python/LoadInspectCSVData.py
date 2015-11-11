# The auctiondata.csv file is there at /user/pravat in MapR-FS. This example has 
# loaded the data and extracted some necessary values using RDD.
# Both./bin/Pyspark or ./spark-submit can be used to perform the operation
# @Pravat 11-11-2015

from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("AuctionsApp")
sc = SparkContext(conf=conf)

#Define indexes for the content in csv file
auctionid = 0
bid = 1
bidtime = 2
bidder = 3
bidderrate = 4
openbid = 5
price = 6
itemtype = 7
daystolive = 8

#To load the file
auctionRDD = sc.textFile("/user/pravat/auctiondata.csv").map(lambda line:line.split(","))

#To see the first element of the RDD
auctionRDD.first

#To see the first 5 elements of the RDD
auctionRDD.take(5)

#What is the total number of bids?
totbids = auctionRDD.count()
print totbids

#What is the total number of distinct items that were auctioned?
totitems = auctionRDD.map(lambda line:line[auctionid]).distinct().count()
print totitems

#What is the total number of item types that were auctioned?
totitemtypes=auctionRDD.map(lambda line:line[itemtype]).distinct().count()
print totitemtypes

#What is the total number of bids per item type?
bids_itemtype = auctionRDD.map(lambda x:(x[itemtype],1)).reduceByKey(lambda x,y:x+y).collect()
print bids_itemtype

#What is the total number of bids per auction?
bids_auctionRDD = auctionRDD.map(lambda x:(x[auctionid],1)).reduceByKey(lambda x,y:x+y)
bids_auctionRDD.take(5) #just to see the first 5 elements

#Across all auctioned items, what is the max number of bids?
maxbids = bids_auctionRDD.map(lambda x:x[bid]).reduce(max)
print maxbids

#Across all auctioned items, what is the minimum of bids?
minbids = bids_auctionRDD.map(lambda x:x[bid]).reduce(min)
print minbids

#What is the average bid?
average_bids = totbids/totitems
print average_bids

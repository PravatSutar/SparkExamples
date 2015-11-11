# The auctiondata.csv file is there at /user/pravat in MapR-FS. This example has 
# loaded the data and extracted some necessary values using Spark DataFrame.
# Both./bin/Pyspark or ./spark-submit can be used to perform the operation
# @Pravat 11-11-2015

#import SQLContext and pyspark SQL functions

from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)

inputRDD = sc.textFile("/user/pravat/auctiondata.csv").map(lambda l: l.split(","))
auctions = inputRDD.map(lambda p:Row(auctionid=p[0], bid=float(p[1]), bidtime=float(p[2]), bidder=p[3], bidrate=int(p[4]), openbid=float(p[5]), price=float(p[6]), itemtype=p[7], dtl=int(p[8])))

# Infer the schema, and register the DataFrame as a table.
auctiondf = sqlContext.createDataFrame(auctions)
auctiondf.registerTempTable("auctions")

auctiondf.show()

auctiondf.printSchema()

totbids = auctiondf.count()
print totbids

totalauctions = auctiondf.select("auctionid").distinct().count()
print total auctions

itemtypes = auctiondf.select("itemtype").distinct().count()
print itemtypes
auctiondf.groupBy("itemtype","auctionid").count().show()
auctiondf.groupBy("itemtype","auctionid").count().agg(func.min("count"), func.max("count"), func.avg("count")).show()
auctiondf.groupBy("itemtype", "auctionid").agg(func.min("bid"), func.max("bid"), func.avg("bid")).show()
auctiondf.filter(auctiondf.price>200).count()
xboxes = sqlContext.sql("SELECT auctionid, itemtype,bid,price,openbid FROM auctions WHERE itemtype = 'xbox'").show()

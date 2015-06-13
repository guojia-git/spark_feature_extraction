from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

#sc = SparkContext()
sqlContext = SQLContext(sc)


# get RDD from data
#df_offer_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../offers.csv")
df_trans_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../train/trans_01.csv")
#df_train_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../trainHistory.csv")
#df_test_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../testHistory.csv")

# for each offer
#offers = df_train_all.select("offer").distinct().map(lambda r: r.offer).collect()
#offer = offers[0]
#offer_row = df_offer_all.filter(df_offer_all.offer == offer)
#
#df_all = sqlContext.createDataFrame(df_train_offer.join(df_trans_offer, df_train_offer.shopperid == df_trans_offer.id, "inner").collect())

execfile("FeatureExtractor.py")

# save features
#df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
import pyspark.sql.functions as functions

#sc = SparkContext()
sqlContext = SQLContext(sc)

# get RDD from data
df_offer_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../offers.csv")
df_trans_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../transactions.csv")
df_train_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../trainHistory.csv")
df_test_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../testHistory.csv")

# for each offer
offers = df_train_all.select("offer").distinct().map(lambda r: r.offer).collect()
offer = offers[0]
offer_row = df_offer_all.filter(df_offer_all.offer == offer)

# for each id
ids = df_train_all.filter(df_train_all.offer == offer).select("id").distinct().map(lambda r: r.id).collect()
id = ids[0]

# prepare the big table for each id
df_train = df_train_all.filter(df_train_all.id == id) #only 1 row
df_trans = df_trans_all.filter(df_trans_all.id == id)
df = sqlContext.createDataFrame(df_train.join(df_trans, df_train.id == df_trans.id, "outer").collect())

# getting feature header

# start implementing features

#row = ()
#row += (feature,)
#rows.append(row)
#df.filter(df.brand==df.offerbrand).count()



# save features
#df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")

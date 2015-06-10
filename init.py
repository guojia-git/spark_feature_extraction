from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

#sc = SparkContext()
sqlContext = SQLContext(sc)

# get RDD from data
df = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../offers.csv")


# save features
#df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")

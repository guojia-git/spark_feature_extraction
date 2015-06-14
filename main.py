#from FeatureExtractor import *
execfile("FeatureExtractor.py")
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

def date_to_int(date):
  #date is in this format: 2012-03-02
  date = date.split("-")
  year = int(date[0])
  is_leap = year % 4 == 0
  month = date[1]
  day = int(date[2])
  
  days_till_month = {"01": 0, \
    "02": 31, \
    "03": 60 if is_leap else 59, \
    "04": 91 if is_leap else 90, \
    "05": 121 if is_leap else 120, \
    "06": 152 if is_leap else 151, \
    "07": 182 if is_leap else 181, \
    "08": 213 if is_leap else 212, \
    "09": 244 if is_leap else 243, \
    "10": 274 if is_leap else 273, \
    "11": 305 if is_leap else 304, \
    "12": 335 if is_leap else 334}
  
  days = (year - 2000) * 365 + int((year - 2000) / 4)
  days += days_till_month[month]
  days += day

#sc = SparkContext()
sqlContext = SQLContext(sc)

# get RDD from data
#df_offer_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../offers.csv")
df_trans_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../train/trans_01.csv")
df_train_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../trainHistory.csv")
df_all = df_trans_all.join(df_train_all, df_trans_all.id == df_train_all.shopperid, "inner")

fe = FeatureExtractor(df_all)

#fe.add_equal_filter(["company=104460040", "brand=7668", "category=1726"])
#fe.add_range_filter("purchaseamount", low="purchasequantity+10")
#fe.add_feature("purchaseamount", functions=["exist"])
fe.add_range_filter("date", low="offerdate-30", function=date_to_int)
fe.add_feature("company", functions=["count"])
f_df = fe.extract()


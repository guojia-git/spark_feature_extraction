from FeatureExtractor import *
#execfile("FeatureExtractor.py")
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
df_trans_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../train/trans_02.csv")
df_train_all = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "../trainHistory.csv")
df_all = df_trans_all.join(df_train_all, df_trans_all.id == df_train_all.shopperid, "inner")

fe = FeatureExtractor(df_all)

fe.add_filter([fe.get_equal_filter("category=3203")], "category")
fe.add_filter([fe.get_equal_filter("company=106414464")], "company")
fe.add_filter([fe.get_equal_filter("brand=13474")], "brand")
ft_date_1 = fe.get_range_filter("date", low="offerdate-30", function=date_to_int)
ft_date_2 = fe.get_range_filter("date", low="offerdate-60", function=date_to_int)
ft_date_3 = fe.get_range_filter("date", low="offerdate-90", function=date_to_int)
fe.add_filter([ft_date_1, ft_date_2, ft_date_3], "date")

fe.add_feature("purchaseamount", functions=["count", "exist"]) # how many times
fe.add_feature("purchaseamount", functions=["sum"]) # how muchmony
fe.add_feature("purchasequantity", functions=["sum"]) # how many times

f_df = fe.extract()
f_df.save("feature_train_01.csv", "com.databricks.spark.csv")

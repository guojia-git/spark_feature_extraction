from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math

sql_types = {"int": "IntegerType()", \
  "float": "DoubleType()", \
  "str": "StringType()", \
  "bool": "BooleanType()"}

is_larger_zero = udf(lambda x: 1 if x > 0 else 0, IntegerType())

class FeatureExtractor:

  def __init__(self, data_frame, id_column="id", filters=[]):
    self.df = data_frame
    self.fltrs = filters
    self.id_col = id_column
    #self.funcs = ["count", "exist"]
    self.funcs = ["count", "max", "avg", "min", "sum", "sumDistinct"]
    self.commands = []

  def __del__(self):
    self.fltrs = "All"
    self.id_col = None
    #self.funcs = ["count", "exist"]
    self.funcs = ["count", "max", "avg", "min", "sum", "sumDistinct"]
    self.commands = []
  
  def get_filter(self, fltr):
      return ".filter('" + fltr + "')"

  def get_function(self, func, col):
    if func == "exist":
      return ".agg(count('" + col + "').alias('result'))\
        .select('" + self.id_col + "', is_larger_zero('result').alias('result'))"
    else:
      return ".agg(" + func + "('" + col + "').alias('result'))"


  def add_filter(self, filters):
    self.fltrs += filters

  def add_feature(self, column, filters=["all"], functions=["all"]):
    # get the list of filters
    fts = []
    if filters == ["all"]:
      fts_separated = self.fltrs
    else:
      fts_separated = filters
    # create all combinations of filters
    num_fts = int(math.pow(2, len(fts_separated)))
    fts_sel = []
    for i in range(0, num_fts):
      fts_sel.append([])
    for i in range(0, len(fts_separated)):
      rnd = int(math.pow(2, i))
      pattern = [True] * rnd + [False] * rnd
      pattern *= int(num_fts / 2 / rnd)
      for j in range(0, num_fts):
        fts_sel[j].append(pattern[j])
    for sel in fts_sel:
      ft = ""
      for i in range (0, len(sel)):
        if sel[i]:
          ft += self.get_filter(fts_separated[i])
      fts.append(ft)

    # get all the functions
    if functions == ["all"]:
      fcs = [self.get_function(x, column) for x in self.funcs]
    else:
      fcs = [self.get_function(x, column) for x in functions]
    # get all the command needed. assume name "df"
    for ft in fts:
      for fc in fcs:
        com = "df" + ft + ".groupBy('" + self.id_col + "')" + fc 
        self.commands.append(com)
    return

  def extract(self):
    df = self.df
    df_f = eval("df.select('" + self.id_col + "').distinct()")
    df_f.show()
    col_names = [self.id_col]
    f_cnt = 0
    for com in self.commands:
      f_name = "feature_" + "%.5d" % f_cnt
      print(f_name)
      print(com)
      print(com)
      print(com)
      print(com)
      print(com)
      print(com)
      print(com)
      df_new = eval(com) # get the new features by executing predefined command
      print(com)
      # Prepare the joind command
      join_com = "df_f.join(df_new, df_f." + self.id_col + " == df_new." + self.id_col + ", 'left_outer')"
      join_com += ".select("
      for cn in col_names:
        join_com += "df_f." + cn + ", "
      join_com += "df_new.result.alias('" + f_name + "'))"
      # Execute command
      df_f = eval(join_com)
      col_names += [f_name]
      f_cnt += 1
    
    df_f = df_f.fillna(0)
    return df_f
    

#from pyspark import SparkContext
#from pyspark.sql import SQLContext
#from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
#from pyspark.sql.functions import *

import math



def get_filter(fltr):
    return ".filter(" + fltr + ")"

def get_column(col, func=""):
    # need to cast string to double
    if func != "count" and func != "exist":
      return "." + col + ".cast('double')"
    else:
      return "." + col

def get_function(func, col):
  if func == "exist":
    return ".agg(count('" + col + "')" + ".collect()[0]." + col + " == 0"
  else:
    return ".agg(" + func + "('" + col + "')" + ".collect()[0]." + col

class FeatureExtractor:
   
  def __init__(self, filters=[]):
     self.fltrs = filters
     self.funcs = ["count", "exist"]
     #self.funcs = ["count", "max", "mean", "min", "sum", "sumDistinct"]
     f_commands = []


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
          ft += get_filter(fts_separated[i])
      fts.append(ft)

    # get all the functions
    if functions == ["all"]:
      fcs = [get_function(x, column) for x in self.funcs]
    else:
      fcs = [get_function(x, column) for x in functions]
    # get all the command needed. assume name "df"
    for ft in fts:
      for fc in fcs:
        com = "df" + ft + fc 
        print(com)

    
    return


fe = FeatureExtractor()  

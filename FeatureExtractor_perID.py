from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math

sql_types = {"int": "IntegerType()", \
  "float": "DoubleType()", \
  "str": "StringType()", \
  "bool": "BooleanType()"}

def get_filter(fltr):
    return ".filter('" + fltr + "')"

def get_column(col, func=""):
    # need to cast string to double
    if func != "count" and func != "exist":
      return "." + col + ".cast('double')"
    else:
      return "." + col

def get_function(func, col):
  if func == "exist":
    return ".agg(count('" + col + "').alias('result'))" + ".collect()[0].result != 0"
  else:
    return ".agg(" + func + "('" + col + "').alias('result'))" + ".collect()[0].result"

class FeatureExtractor:

  def __init__(self, data_frame, id_column="id", filters=[]):
    self.df_all = data_frame
    self.fltrs = filters
    self.id_col = id_column
    #self.funcs = ["count", "exist"]
    self.funcs = ["count", "max", "avg", "min", "sum", "sumDistinct"]
    self.commands = []

  def __del__(self):
    self.df_all = None
    self.fltrs = "All"
    self.id_col = None
    #self.funcs = ["count", "exist"]
    self.funcs = ["count", "max", "avg", "min", "sum", "sumDistinct"]
    self.commands = []
  
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
        self.commands.append(com)
    return

  def extract(self):
    features = []
    # obtain ids, to separate the data frame into small chunks
    ids = self.df_all.select(self.id_col).distinct().map(lambda r: r.id).collect()
    #ids = ["123213131"]
    # get data for each id
    for id in ids:
      feature_point = ()

      with open ("record.txt", "a") as f:
        f.write(id + '\n')
        
        df = self.df_all.filter("id=" + id)
        for com in self.commands:
          feature_point += (eval(com),)
          f.write(com + '\n')

        features.append(feature_point)
        f.write('\n')
    
    # infer schema
    # get types for each feature
    feature_point = features[0]
    types = ()
    for fp in feature_point:
      types += (type(fp).__name__, )
    # ceate a command in string, then exec it
    schema_command = "StructType(["
    feature_numbering = math.log(len(types), 10)
    if feature_numbering > 5:
      print("WARNING: Too many features")
    for i in range(0, len(types)):
      tp = sql_types[types[i]]
      name = "feature_" + "%.5d" % i
      schema_command += "StructField('" + name + "', " + tp + ", False),"
    schema_command += "])"
    schema = eval(schema_command)

    return sqlContext.createDataFrame(sc.parallelize(features), schema)
    

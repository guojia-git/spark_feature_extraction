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


def parse_expr(expr):
  if expr.find("+") > 0:
    return expr.split("+") + ["+"]
  elif expr.find("-") > 0:
    return expr.split("-") + ["-"]
  elif expr.find("*") > 0:
    return expr.split("*") + ["*"]
  elif expr.find("/") > 0:
    return expr.split("/") + ["/"]
  else:
    return [expr]
  

class FeatureExtractor:
  def __init__(self, data_frame, id_column="id", filters={}):
    self.df = data_frame
    self.fltrs = filters
    self.id_col = id_column
    #self.funcs = ["count", "exist"]
    self.funcs = ["count", "max", "avg", "min", "sum", "sumDistinct"]
    self.commands = []
    self.udfs = []
    self.defs = []

  def __del__(self):
    self.fltrs = {}
    self.id_col = None
    #self.funcs = ["count", "exist"]
    self.funcs = ["count", "max", "avg", "min", "sum", "sumDistinct"]
    self.commands = []
    self.udfs = []
    self.defs = []
  
  def get_function(self, func, col):
    if func == "exist":
      return ".agg(count('" + col + "').alias('result'))\
        .select('" + self.id_col + "', is_larger_zero('result').alias('result'))"
    else:
      return ".agg(" + func + "('" + col + "').alias('result'))"

  def add_filter(self, filters, name):
    self.fltrs[name] = filters

  def get_equal_filter(self, expr):
     return ".filter('" + expr + "')"

  def get_range_filter(self, column, low=None, high=None, function=lambda x: float(x)):
    # record function
    udf_idx = len(self.udfs)
    self.udfs.append(udf(function, DoubleType()))
    self.defs.append(function)
    # get low and high
    if low != None:
      #if it's a column
      expr = parse_expr(low)
      if len(expr) == 1:
        fltr_low = "self.udfs[" + str(udf_idx) + "]('" + column + "').alias('" + column + "')"\
          " > " +\
          "self.defs[" + str(udf_idx) + "]('" + expr[0] + "').alias('" + expr[0] + "')"
      else:
        fltr_low = "self.udfs[" + str(udf_idx) + "]('" + column + "').alias('" + column + "')" +\
          " > " +\
          "self.udfs[" + str(udf_idx) + "]('" + expr[0] + "').alias('" + expr[0] + "')" + expr[2] + expr[1]
    else:
      fltr_low = ""
    
    if high != None:
      #if it's a column
      expr = parse_expr(high)
      if len(expr) == 1:
        fltr_high = "self.udfs[" + str(udf_idx) + "]('" + column + "').alias('" + column + "')" +\
          " < " +\
          "self.defs[" + str(udf_idx) + "]('" + expr[0] + "').alias('" + expr[0] + "')"
      else:
        fltr_high = "self.udfs[" + str(udf_idx) + "]('" + column + "').alias('" + column + "')" +\
          " > " +\
          "self.udfs[" + str(udf_idx) + "]('" + expr[0] + "').alias('" + expr[0] + "')" + expr[2] + expr[1]
    else:
      fltr_high = ""
    fltr = ""
    if fltr_low:
      fltr += ".filter(" + fltr_low + ")"
    if fltr_high:
      fltr += ".filter(" + fltr_high + ")"
    return fltr
    #self.fltrs += [fltr]
    

  def add_feature(self, column, filters=["all"], functions=["all"]):
    # get the list of filters
    fts = []
    if filters == ["all"]:
      ftg_keys = self.fltrs.keys()
    else:
      ftg_keys = filters
    # create all combinations of filters
    # filter groups with 3, 2, 3 filters respectively
    # 3: 4 cases, [True, False, False], [F,T,F], [F,F,T],[F,F,F]
    # total (3+1) * (2+1) * (3+1) filters
    num_fts = 1
    ftg_lens = []
    for key in ftg_keys:
      length = len(self.fltrs[key])
      ftg_lens.append(length)
      num_fts *= (length + 1)

    fts = [] #One for each combination. e.g. [[T,F,F], [T,F], [F,F,F]] 
    for i in range(0, num_fts):
      fts.append("")
    cycle_len = num_fts
    for i in range(0, len(ftg_keys)): #Per filter group
      ftg_len = ftg_lens[i]
      cycle_len /= (ftg_len + 1)
      #Prepare each group
      ftg_sel = [[False for j in range(ftg_len)] for k in range(ftg_len + 1)] 
      for j in range(0, ftg_len):
        ftg_sel[j][j] = True
      #Create assignment sequence by cycling  
      cycle = []
      for j in range(ftg_len + 1):
        cycle += [ftg_sel[j]] * cycle_len 
      full = cycle * (num_fts / len(cycle))
      #Put the sequence to the real filter vector
      ftg = self.fltrs[ftg_keys[i]]
      #Add to the filter string per feature
      for j in range(len(full)):
        for k in range(len(full[j])):
          sel = full[j][k]
          if sel:
            fts[j] += ftg[k]

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
      print(com)
      print(com)
      print(com)
      print(com)
      print(com)
      print(com)
      print(com)
      print(self.udfs[0])
      print(com)
      df_new = eval(com) # get the new features by executing predefined command
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
    

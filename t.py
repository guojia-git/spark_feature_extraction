execfile("init.py")
execfile("FeatureExtractor.py")

def date_to_int(date):
  print(date)
  print(date)
  print(date)
  print(date)
  print(date)
  print(date)
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



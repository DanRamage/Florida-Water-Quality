from datetime import datetime, timedelta, date
from pytz import timezone


def datetime2matlabdn(dt):
   mdn = dt + timedelta(days = 366)
   frac_seconds = (dt-datetime(dt.year,dt.month,dt.day,0,0,0,0,timezone('UTC'))).seconds / (24.0 * 60.0 * 60.0)
   frac_microseconds = dt.microsecond / (24.0 * 60.0 * 60.0 * 1000000.0)
   return mdn.toordinal() + frac_seconds + frac_microseconds

def matlabdntodatetime(matlab_dn):
  #Get the fractional days from the matlab_dn
  fract_days = Decimal(matlab_dn) % 1

  py_dt = datetime.fromordinal(int(matlab_dn)) + timedelta(days=float(fract_days)) - timedelta(days=366)

  return py_dt

def get_utc_epoch(datetime_val):
  DAY = 24*60*60 # POSIX day in seconds (exact value)
  date_part = datetime_val.date()
  epoch_time = (date_part.toordinal() - date(1970, 1, 1).toordinal()) * DAY
  epoch_time = (date_part - date(1970, 1, 1)).days * DAY + (datetime_val.hour * 60 * 60 + datetime_val.minute * 60 + datetime_val.second)

  return epoch_time
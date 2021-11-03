import datetime


def to_datetime(date):
    if type(date) == str:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    return date

def get_yesterday(is_UTC0=False):
    today = datetime.datetime.today()
    if is_UTC0:
        today = today + datetime.timedelta(hours=8)
        yesterday = today - datetime.timedelta(days=1 ,hours=today.hour ,minutes=today.minute ,seconds=today.second
                                               ,microseconds=today.microsecond)
    else:
        yesterday = today - datetime.timedelta(days=1 ,hours=today.hour ,minutes=today.minute ,seconds=today.second
                                               ,microseconds=today.microsecond)
    return yesterday

def get_today(is_UTC0=False): # YYYY-mm-dd-0-0
    today = datetime.datetime.today()
    if is_UTC0:
        today = today + datetime.timedelta(hours=8)
        today_zero = today - datetime.timedelta(days=0 ,hours=today.hour ,minutes=today.minute ,seconds=today.second
                                               ,microseconds=today.microsecond)
    else:
        today_zero = today - datetime.timedelta(days=0 ,hours=today.hour ,minutes=today.minute ,seconds=today.second
                                               ,microseconds=today.microsecond)

    # today_zero = today - datetime.timedelta(days=0 ,hours=today.hour ,minutes=today.minute ,seconds=today.second
    #                                         ,microseconds=today.microsecond)
    return today_zero

def get_hour(is_UTC0=False):
    today = datetime.datetime.today()
    if is_UTC0: # add 8 hour to UTC+8
        today = today + datetime.timedelta(hours=8)
        return today.hour
    else:
        return today.hour

def get_date_shift(days=0, to_str=False, pattern='%Y-%m-%d', is_UTC0=False): # YYYY-mm-dd-0-0
    today = datetime.datetime.today()
    if is_UTC0: # add 8 hour to UTC+8
        today = today + datetime.timedelta(hours=8)
    date_shift = today - datetime.timedelta(days=days ,hours=today.hour ,minutes=today.minute ,seconds=today.second
                                            ,microseconds=today.microsecond)
    if to_str:
        date_shift = datetime_to_str(date_shift, pattern=pattern)
    return date_shift

def get_days_of_month(date):
    if type(date) == str:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    year = date.year
    month = date.month
    d = datetime.date(year + int(month / 12), month % 12 + 1, 1) - datetime.timedelta(days=1)
    return d.day


def datetime_to_str(date, pattern='%Y-%m-%d'):
    datetime_str = datetime.datetime.strftime(date, pattern)
    return datetime_str


def date2int(date, sep='-'):
    if type(date) == datetime.datetime:
        date_str = datetime_to_str(date)
        date_int = int(''.join(date_str.split(sep)))
    elif type(date) == int:
        date_int = date
    else:
        date_str = date
        date_int = int(''.join(date_str.split(sep)))
    return date_int



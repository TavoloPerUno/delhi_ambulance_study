from random import randrange
from datetime import timedelta
import datetime
import math
import numpy as np

def get_next_weekday(startdate, weekday):
    """
    @startdate: given date, in format '2013-05-25'
    @weekday: week day as a integer, between 0 (Monday) to 6 (Sunday)
    """
    t = timedelta(days = (datetime.datetime.now(startdate.tzinfo) - startdate).days + 1 + timedelta((7 + weekday - datetime.datetime.now(startdate.tzinfo).weekday()) % 7).days)

    return (startdate + t)

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def uniq(lst):
    last = object()
    for item in lst:
        if item == last:
            continue
        yield item
        last = item

def sort_and_deduplicate(l):
    return list(uniq(sorted(l, reverse=True)))

def tidy_maxspeed_tuple_to_int(val):
    if type(val) == list:
        return min(map(float, val))
    elif type(val) == str:
        return float(val)
    elif math.isnan(val):
        return np.nan

def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out
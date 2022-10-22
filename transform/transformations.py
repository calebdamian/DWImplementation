from datetime import datetime
from decimal import Decimal
from pickletools import decimalnl_short
from unicodedata import decimal


def str_to_int(str1):
    try:
        val = int(str1)
    except:
        val = None
        print("Invalid string")
    return val


def str_to_char(str1):
    return str1[0]


def str_to_str_w_length(str1, length):
    if len(str1) > length:
        return str1[:length]
    elif len(str1) == length or len(str1) < length:
        return str1
    else:
        print("Error at str_to_str_w_length")


def str_to_date(str1):
    return datetime.date(str1, "%d-%m-%Y")


def str_to_float(str1):
    try:
        val = float(str1)
    except:
        val = None
        print("Invalid string at str_to_float")
    return val

from datetime import datetime


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
    try:
        if len(str1) > length:
            return str1[:length]
        elif len(str1) == length or len(str1) < length:
            return str1
        elif len(str1) == None:
            return None
    except:
        print("String is empty")
        return None


def str_to_date(str1):
    return datetime.strptime(str1, "%d-%b-%y")


def str_to_float(str1):
    try:
        val = float(str1)
    except:
        val = None
        print("Invalid string at str_to_float")
    return val


def get_month_name(str1):
    val = datetime.strptime(str1, "%m")
    return val.strftime("%B")

import datetime


def get_current_date():
    return datetime.datetime.now().strftime('%Y-%m-%d')

def get_current_time():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def format_datetime_to_str(datatime_obj, format_str='%Y-%m-%d %H:%M:%S'):
    return datatime_obj.strftime(format_str)


def format_date_to_str(data_obj, format_str='%Y-%m-%d'):
        return data_obj.strftime(format_str)
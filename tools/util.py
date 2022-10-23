import uuid
import random
import json
import hashlib
import datetime
from flask.json import JSONEncoder as BaseJSONEncoder
import decimal
from flask import request
from tools.time_tool import get_current_time


def md5(basestr, case="lower"):
    """
    求字符串的md5值，默认是小写，如果是大写第二个参数传递upper等。
    """
    md = hashlib.md5()  # 创建md5对象
    md.update(basestr.encode(encoding='utf-8'))
    if case == "lower":
        return md.hexdigest().lower()
    else:
        return md.hexdigest().upper()


class MyJsonEncoder(BaseJSONEncoder):
    def default(self, o):
        """
        如有其他的需求可直接在下面添加
        :param o:
        :return:
        """
        if isinstance(o, datetime.datetime):
            # 格式化时间
            return o.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(o, datetime.date):
            # 格式化日期
            return o.strftime('%Y-%m-%d')
        if isinstance(o, decimal.Decimal):
            # 格式化高精度数字
            return str(o)
        if isinstance(o, uuid.UUID):
            # 格式化uuid
            return str(o)
        if isinstance(o, bytes):
            # 格式化字节数据
            return o.decode("utf-8")

        # if isinstance(o, ObjectId):
        #     return str(o)
        # if isinstance(o, EmbeddedDocument):
        #     return o.to_mongo()

        return super(MyJsonEncoder, self).default(o)


class JsonTool():

    @staticmethod
    def to_json(srcdict, cls=MyJsonEncoder, ensure_ascii=False):
        return json.dumps(srcdict, cls=cls, ensure_ascii=ensure_ascii)

    @staticmethod
    def to_str_json(srcdict, cls=MyJsonEncoder, ensure_ascii=False):
        return json.dumps(srcdict, cls=cls, ensure_ascii=ensure_ascii)

    @staticmethod
    def to_dict_or_list(jsonstr):
        try:
            retdict = json.loads(jsonstr)
            if isinstance(retdict, dict) or isinstance(retdict, list):
                return retdict
            else:
                raise ValueError("不是合法的json字符串")
        except:
            # 没有load成功可能传入了dictstr
            try:
                retdict = eval(jsonstr)
                if isinstance(retdict, dict) or isinstance(retdict, list):
                    return retdict
                else:
                    raise ValueError("不是合法的dict字符串")
            except:
                raise ValueError("不是json也不是dictstr。")

    @staticmethod
    def check_jsonable(jsonstr):
        try:
            retdict = json.loads(jsonstr)
            if isinstance(retdict, dict) or isinstance(retdict, list):
                return True
            else:
                return False
        except:
            try:
                retdict = eval(jsonstr)
                if isinstance(retdict, dict) or isinstance(retdict, list):
                    return True
                else:
                    return False
            except:
                return False


def get_global_trace_id():
    try:

        if request and request.headers.get("TRACE_ID"):
            log_trace_id = request.headers.get("TRACE_ID", "0")
        else:
            log_trace_id = md5(get_current_time() + str(random.randint(0, 1000000000)))
    except:
        log_trace_id = md5(get_current_time() + str(random.randint(0, 1000000000)))
    return log_trace_id


def get_sub_string(params, start, end):
    find_start_tag = start
    find_end_tag = end
    if find_start_tag == "":
        spos = 0
    else:
        spos = params.find(find_start_tag)

    if spos == -1:  # 没有发现关键字 SQL_SELECT()
        return ''

    if find_end_tag == "":
        epos = len(params)
    else:
        epos = params.find(find_end_tag, spos + len(find_start_tag))

    if epos == -1:  # 没有发现关键字 find_end_tag
        return ''

    return params[spos + len(find_start_tag):epos]


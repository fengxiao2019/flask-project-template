from ast import Return
import json
import datetime


class ReturnDict(object):
    # -1：未自定义的异常
    CODE_EXCEPTION = -1
    CODE_SUCCESS = 0

    def __init__(self, code=CODE_SUCCESS, msg="success", data=None):
        self.__code = code
        self.__msg = msg
        self.__data = data

    @property
    def code(self):
        return self.__code

    @code.setter
    def code(self, code):
        self.__code = code

    @property
    def msg(self):
        return self.__msg

    @msg.setter
    def msg(self, msg):
        self.__msg = msg

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, data):
        if isinstance(data, bytes):
            data = str(data, encoding="utf-8")
        self.__data = data

    def to_json(self, is_unicode=False):
        from tools.util import MyJsonEncoder
        return json.dumps(self.to_dict(), ensure_ascii=is_unicode, cls=MyJsonEncoder)

    def to_dict(self):
        ret_dict = {}
        ret_dict['code'] = self.code
        ret_dict['msg'] = self.msg
        if isinstance(self.data, bytes):
            self.data = str(self.data, encoding="utf-8")
        if not isinstance(self.data, dict) \
                and not isinstance(self.data, list) \
                and not isinstance(self.data, str) \
                and not isinstance(self.data, int) \
                and not isinstance(self.data, float) \
                and not isinstance(self.data, type(None)):
            ret_dict['data'] = "不识别的data类型！data必须是dict，list或者str/int/float。"
        else:
            if isinstance(self.data, dict):
                for k, v in self.data.items():
                    if isinstance(v, datetime.datetime):
                        # 如果是datetime类型的，无法转换为json，要先转换为字符串
                        self.data[k] = str(v)

            ret_dict['data'] = self.data
        return ret_dict


class StandardError(ValueError):
    def __init__(self, msg, code=ReturnDict.CODE_EXCEPTION, data=None, status_code=200):
        super(StandardError, self).__init__()
        self.msg = msg
        self.code = code
        self.data = data
        self.status_code = status_code

    def __str__(self):
        return f"raise {self.__name__}：{self.to_dict()}"

    def to_dict(self):
        return ReturnDict(code=self.code, msg=self.msg, data=self.data).to_dict()

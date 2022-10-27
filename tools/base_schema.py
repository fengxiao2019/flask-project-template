from typing import Any, Dict
from marshmallow.exceptions import ValidationError
from marshmallow import fields, Schema
import functools
from flask import request
from marshmallow import Schema, fields
from marshmallow.exceptions import ValidationError
from marshmallow.validate import Length


_ObjectDictBase = Dict[str, Any]


class ObjectDict(_ObjectDictBase):
      """Makes a dictionary behave like an object, with attribute-style access.
      """

      def __getattr__(self, name):
          # type: (str) -> Any
          try:
              return self[name]
          except KeyError:
              raise AttributeError(name)

      def __setattr__(self, name, value):
          # type: (str, Any) -> None
          self[name] = value


class BaseDataSchema(Schema):
    """
    仅适用于第一层字段的数据格式校验，由多层校验时，err_text
    err_text:适用于提示前端错误字段
    errors: 详细的错误字典数据
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validate_res = None
        self.dict_object = None
        self.errors = None
        self.err_text = ""
        self.valid_data = None

    @property
    def storage(self):
        return self.dict_object

    def load(self, *args, **kwargs):
        try:
            self.validate_res = super().load(*args, **kwargs)
            self.dict_object = ObjectDict(self.validate_res)
            self.error_messages
        except ValidationError as e:
            self.errors = e.messages
            self.err_text = self.get_err_val(self.errors)
            self.valid_data = e.valid_data
        return self

    def get_err_val(self, errors):
        # 如果_schema 没有出现在self.errors中，直接返回错误的参数key
        # 如果_schema 出现在其中，返回_schema 对应的value
        error_text = "参数错误: "
        if '_schema' in self.errors:
            return error_text + ", ".join(self.errors['_schema'])
        return error_text + ", ".join(self.errors.keys())


# def specify_schema(schema_class, user_id_flag=False, real_name_flag=False, permission_key=None, post_flag=True):
#     def get_user_info(func):
#         @functools.wraps(func)
#         def wrapper(self, *args, **kwargs):
#             if post_flag:
#                 cur_params = request.json or {}
#             else:
#                 cur_params = dict(request.args)
#             if user_id_flag or real_name_flag or permission_key:
#                 user_id, real_name = CommonTool.get_user_id_real_name()
#                 if user_id_flag and not cur_params.get('user_id'):
#                     cur_params['user_id'] = user_id
#                 if real_name_flag and not cur_params.get('real_name'):
#                     cur_params['real_name'] = real_name

#                 if permission_key:
#                     if not validate_permission(user_id, 'admin') and \
#                             not validate_permission(user_id, permission_key):
#                         raise StandardError(msg='无此操作权限')
#             schema_obj = schema_class().load(cur_params)
#             if schema_obj.errors:
#                 user_logger.info(schema_obj.errors)
#                 raise StandardError(msg=schema_obj.err_text)
#             self.schema_obj = schema_obj
#             self.cur_params = schema_obj.storage
#             return func(self, *args, **kwargs)

#         return wrapper

#     return get_user_info

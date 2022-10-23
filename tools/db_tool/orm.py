import datetime
import decimal

from tools.db_tool.orm_base import BaseOrm
from tools.util import JsonTool
from tools.exception import StandardError
from tools.util import get_sub_string


class DORM(BaseOrm):

    # 构造函数 初始化方法
    def __init__(self, table_name, table_alias="", logger_errors=True, fields=None,
                 values=None, start_transaction=False, force_execute=False,
                 **kwargs):
        # table_name 表名
        # id 表示主键
        self.table_name = table_name
        self.table_alias = table_alias
        self.logger_errors = logger_errors
        self.reset_sql_info()
        self.force_execute = force_execute
        self.set_attrs(**kwargs)
        self.fields = fields
        self.values = values
        self.start_transaction = start_transaction

    def __repr__(self):
        return JsonTool.to_json(self.get_attrs(whether_jsonable=True))

    def set(self, name, value):
        setattr(self, name, value)

    def get(self, name):
        return getattr(self, name)

    def set_attrs(self, **kwargs):
        for tmpk, tmpv in kwargs.items():
            self.set(tmpk, tmpv)

    def get_attrs(self, whether_jsonable=False):
        attrdict = {}
        for tmpkey, tmpvalue in self.__dict__.items():
            if tmpkey not in self.except_attr_list():
                if whether_jsonable:
                    if isinstance(tmpvalue, datetime.datetime) or isinstance(tmpvalue, datetime.date):
                        tmpvalue = str(tmpvalue).split(".")[0]
                attrdict[tmpkey] = tmpvalue
        return attrdict

    # 对象内部调用函数
    def reset_sql_info(self):
        self._sql_info = {
            "do": "",
            "where": "",
            "group_by": "",
            "order_by": "",
            "limit": "",
            "lock": "",
            "join_list": [],
        }

    def validate_saveable(self):
        pass

    def except_attr_list(self):
        return ["table_name", "table_alias", "_sql_info", "logger_errors", "fields", "values", "start_transaction",
                'force_execute']

    def clear_attrs(self):
        dict_keys = list(self.__dict__.keys())
        for tmp_key in dict_keys:
            if tmp_key not in self.except_attr_list():
                del self.__dict__[tmp_key]

    def set_where_str(self, condition_dict, table_alias="", specify_table=False, condition_tag="AND"):
        """
        "gt", "gte", "lt", "lte", "in", "notin",
        "contains", "ne", "like", "notlike", "rlike", "llike"
        :param condition_dict:
        :param table_alias:
        :param specify_table: 是否指定表,默认使用this.table,为True时可以在自己的条件中自己拼接表
        :param condition_tag: 条件 AND 或者 OR，默认AND
        :return:
        """
        if self.check_sql_is_insert():
            raise ValueError("INSERT不需要使用where子句！")

        wherestr = ""
        if not condition_dict:
            return wherestr
        for col, value in condition_dict.items():
            collist = DORM.escape_string_for_sql(col).split("__")
            col = collist[0]
            if col in self.except_attr_list():
                continue
            judge_value = "="
            if len(collist) >= 2 and collist[-1] in [
                "gt", "gte", "lt", "lte", "in", "notin",
                "contains", "ne", "like", "notlike", "rlike", "llike"
            ]:
                for colindex in range(1, len(collist) - 1):
                    col += "__%s" % collist[colindex]
                if collist[-1] == "gt":
                    judge_value = ">"
                elif collist[-1] == "gte":
                    judge_value = ">="
                elif collist[-1] == "lt":
                    judge_value = "<"
                elif collist[-1] == "lte":
                    judge_value = "<="
                elif collist[-1] == "ne":
                    judge_value = "!="
                elif collist[-1] == "in":
                    judge_value = "IN"
                elif collist[-1] == "notin":
                    judge_value = "NOT IN"
                elif collist[-1] == "contains":
                    judge_value = "LIKE"
                elif collist[-1] == "like":
                    judge_value = "LIKE"
                elif collist[-1] == "notlike":
                    judge_value = "NOT LIKE"
                elif collist[-1] == "rlike":
                    judge_value = "RLIKE"
                elif collist[-1] == "llike":
                    judge_value = "LLIKE"

            # 特殊判断列表
            condition_in_list = ["IN", "NOT IN"]
            condition_like_list = ["LIKE", "NOT LIKE", "RLIKE", "LLIKE"]

            # 拼接value
            condition_tag_len = len(condition_tag)
            bak_col = col
            if not specify_table:
                col = f"`{self.table_name}`.`{col}`" if not table_alias else f"`{table_alias}`.`{col}`"

            if isinstance(value, int) or isinstance(value, float):
                if judge_value in condition_in_list or judge_value in condition_like_list:
                    raise ValueError("值为int类型的条件不可进行IN或者LIKE查询！")
                wherestr += f"""{col} {judge_value} {value} {condition_tag} """
            elif isinstance(value, str):
                value_processed = DORM.escape_string_for_sql(value).replace("%", "\\%")
                if judge_value in condition_in_list:
                    raise ValueError("值为str类型的条件不可进行IN查询！")
                if judge_value in ["LIKE", "NOT LIKE"]:
                    wherestr += f"""{col} {judge_value} '%{value_processed}%' {condition_tag} """
                elif judge_value == "RLIKE":
                    wherestr += f"""{col} LIKE '{value_processed}%' {condition_tag} """
                elif judge_value == "LLIKE":
                    wherestr += f"""{col} LIKE '%{value_processed}' {condition_tag} """
                else:
                    wherestr += f"""{col} {judge_value} '{value_processed}' {condition_tag} """
            elif isinstance(value, datetime.datetime):
                if judge_value in condition_in_list or judge_value in condition_like_list:
                    raise ValueError("值为datetime类型的条件不可进行IN或者LIKE查询！")
                wherestr += f"""{col} {judge_value} '{value.strftime('%Y-%m-%d %H:%M:%S')}' {condition_tag} """
            elif isinstance(value, datetime.date):
                if judge_value in condition_in_list or judge_value in condition_like_list:
                    raise ValueError("值为date类型的条件不可进行IN或者LIKE查询！")
                wherestr += f"""{col} {judge_value} '{value.strftime('%Y-%m-%d')}' {condition_tag} """
            elif isinstance(value, list):
                if judge_value in condition_in_list:
                    valuestr = ""
                    for tmpvalue in value:
                        if isinstance(tmpvalue, int) or isinstance(tmpvalue, float):
                            valuestr += "%s," % tmpvalue
                        elif isinstance(tmpvalue, str):
                            valuestr += "'%s'," % DORM.escape_string_for_sql(tmpvalue)
                        elif tmpvalue is None:
                            valuestr += "NULL,"

                    valuestr = valuestr.strip(",")
                    wherestr += f"""{col} {judge_value} ({valuestr}) {condition_tag} """
                elif judge_value in ["LIKE", "NOT LIKE"]:
                    wherestr += f"""{col} {judge_value} '%{DORM.escape_string_for_sql(JsonTool.to_json(value))}%' {condition_tag} """
                elif judge_value == "RLIKE":
                    wherestr += f"""{col} LIKE '{DORM.escape_string_for_sql(JsonTool.to_json(value))}%' {condition_tag} """
                elif judge_value == "LLIKE":
                    wherestr += f"""{col} LIKE '%{DORM.escape_string_for_sql(JsonTool.to_json(value))}' {condition_tag} """
                else:
                    wherestr += f"""{col} {judge_value} '{DORM.escape_string_for_sql(JsonTool.to_json(value))}' {condition_tag} """
            elif isinstance(value, dict):
                if judge_value in condition_in_list:
                    raise ValueError("值为dict类型的条件不可进行IN查询！")
                if judge_value in ["LIKE", "NOT LIKE"]:
                    wherestr += f"""{col} {judge_value} '%{DORM.escape_string_for_sql(JsonTool.to_json(value))}%' {condition_tag} """
                elif judge_value == "RLIKE":
                    wherestr += f"""{col} LIKE '{DORM.escape_string_for_sql(JsonTool.to_json(value))}%' {condition_tag} """
                elif judge_value == "LLIKE":
                    wherestr += f"""{col} LIKE '%{DORM.escape_string_for_sql(JsonTool.to_json(value))}' {condition_tag} """
                else:
                    wherestr += f"""{col} {judge_value} '{DORM.escape_string_for_sql(JsonTool.to_json(value))}' {condition_tag} """
            elif value is None:
                if judge_value in condition_in_list or judge_value in condition_like_list:
                    raise ValueError("值为int类型的条件不可进行IN或者LIKE查询！")
                wherestr += f"""{col} {judge_value} NULL {condition_tag} """
            else:
                raise ValueError("不支持的值类型(%s)" % type(value))

        wherestr = wherestr.strip()[:-1 * condition_tag_len] if wherestr else ""
        if condition_tag == "OR":
            # 如果是OR的时候，需要加外括号
            wherestr = f"({wherestr})"
        if not self._sql_info["where"]:
            self._sql_info["where"] = wherestr if wherestr == "" else "WHERE %s" % wherestr
        else:
            # 说明已经有where了，补充条件
            self._sql_info["where"] += f" AND {wherestr}" if wherestr else ""
        return wherestr

    def get_update_str(self, update_dict):
        # 进入where更新
        if not isinstance(update_dict, dict):
            raise ValueError("执行条件更新时update_dict必须是字典！")
        udpatestr = ""
        for col, value in update_dict.items():
            col = DORM.escape_string_for_sql(col)
            if col in self.except_attr_list():
                continue
            # 拼接value
            if isinstance(value, int) or isinstance(value, float):
                udpatestr += "`%s`=%s," % (col, value)
            elif isinstance(value, str):
                udpatestr += "`%s`='%s'," % (col, DORM.escape_string_for_sql(value))
            elif isinstance(value, datetime.datetime):
                udpatestr += "`%s`='%s'," % (col, value.strftime('%Y-%m-%d %H:%M:%S'))
            elif isinstance(value, datetime.date):
                udpatestr += "`%s`='%s'," % (col, value.strftime('%Y-%m-%d'))
            elif isinstance(value, list) or isinstance(value, dict):
                udpatestr += "`%s`='%s'," % (col, DORM.escape_string_for_sql(JsonTool.to_json(value)))
            elif isinstance(value, decimal.Decimal):
                udpatestr += "`%s`=%s," % (col, float(value))
            elif value is None:
                # 应该写空字符串还是数字0，还是不处理？ 暂时不处理
                pass
            else:
                raise ValueError("不支持的值类型(%s)" % type(value))
        udpatestr = udpatestr.strip(",")
        return udpatestr

    def sync_attr_to_self(self, other_orm_model):
        for k, v in other_orm_model.__dict__.items():
            setattr(self, k, v)

    # 各种操作类方法，增删改查的初始化
    def save(self, force_insert=False, force_update=False):
        if force_insert:
            return self.insert()
        if force_update:
            if hasattr(self, "id"):
                return self.update()
            else:
                raise ValueError("force_update时必须有主键id")
        if hasattr(self, "id"):
            return self.update()
        else:
            return self.insert()

    def delete(self):
        self.reset_sql_info()
        self._sql_info["do"] = "DELETE FROM %s" % (self.table_name)
        self.set_where_str(self.__dict__, table_alias=self.table_alias)
        return self

    def check_sql_is_select(self):
        current_do = self._sql_info["do"].split(" ")[0]
        if current_do == "SELECT":
            return True
        else:
            return False

    def check_sql_is_insert(self):
        current_do = self._sql_info["do"].split(" ")[0]
        if current_do == "INSERT":
            return True
        else:
            return False

    def check_sql_is_update(self):
        current_do = self._sql_info["do"].split(" ")[0]
        if current_do == "UPDATE":
            return True
        else:
            return False

    def check_sql_is_delete(self):
        current_do = self._sql_info["do"].split(" ")[0]
        if current_do == "DELETE":
            return True
        else:
            return False

    def select(self, col_str="*"):
        return self.query(col_str=col_str)

    def query(self, col_str="*"):
        self.reset_sql_info()
        if col_str == "*":
            if self.table_alias:
                col_str = f"`{self.table_alias}`.*"
            else:
                col_str = f"`{self.table_name}`.*"

        self._sql_info["do"] = f"SELECT {col_str} FROM {self.table_name} {self.table_alias} "
        self.set_where_str(self.__dict__, table_alias=self.table_alias)
        return self

    def distinct(self):
        if self.check_sql_is_select() is False:
            raise ValueError("只有SELECT的时候才可以使用distinct！")
        self._sql_info["do"] = f'{self._sql_info["do"][0:6]} DISTINCT{self._sql_info["do"][6:]}'
        return self

    def query_count(self, query_column='*'):
        self.reset_sql_info()
        self._sql_info["do"] = "SELECT COUNT(%s) AS query_count_result FROM %s" % (query_column, self.table_name)
        self.set_where_str(self.__dict__, table_alias=self.table_alias)
        result = self.execute()
        if len(result) > 0:
            return result[0].get('query_count_result')
        else:
            return 0

    def insert(self, **insert_attrs):
        self.reset_sql_info()
        self.validate_saveable()
        clostr, valuestr = "", ""
        self.__dict__.update(insert_attrs)
        for col, value in self.__dict__.items():
            if col in self.except_attr_list():
                continue
            # 拼接列
            clostr += "`%s`," % DORM.escape_string_for_sql(col)
            # 拼接value
            if isinstance(value, int) or isinstance(value, float):
                valuestr += "%s," % value
            elif isinstance(value, str):
                valuestr += "'%s'," % DORM.escape_string_for_sql(value)
            elif isinstance(value, datetime.datetime):
                valuestr += "'%s'," % value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, datetime.date):
                valuestr += "'%s'," % value.strftime('%Y-%m-%d')
            elif isinstance(value, list) or isinstance(value, dict):
                valuestr += "'%s'," % DORM.escape_string_for_sql(JsonTool.to_json(value))
            elif isinstance(value, decimal.Decimal):
                valuestr += "%s," % (float(value))
            elif value is None:
                valuestr += "NULL,"
            else:
                raise ValueError("不支持的值类型(%s)" % type(value))

        clostr = clostr.strip(",")
        valuestr = valuestr.strip(",")
        self._sql_info["do"] = f"INSERT INTO {self.table_name}({clostr}) VALUES ({valuestr})"
        return self

    def batch_insert(self, fields=None, values=None):
        """

        :param fields: 列名，例如 ["id", "name"]
        :param values: 值双层列表，内层列表长度与fields一一对应。例如: [[1,"wang"], [2, "liu"]]
        :return:
        """
        self.reset_sql_info()
        self.validate_saveable()
        colstr, valuestr = "", ""
        if not self.fields:
            if not fields:
                raise ValueError("必须要有 fields 且 不能为空")
            else:
                self.fields = fields
        if not self.values:
            if not values:
                raise ValueError("必须要有 values 且 不能为空")
            else:
                self.values = values

        for field in self.fields:
            colstr += "`%s`," % field
        for value in self.values:
            if len(value) != len(self.fields):
                raise ValueError("字段长度与值长度不一致")
            paramstr = ""
            for index, param in enumerate(value):
                if isinstance(param, int) or isinstance(param, float):
                    paramstr += "%s," % param
                elif isinstance(param, str):
                    paramstr += "'%s'," % DORM.escape_string_for_sql(param)
                elif isinstance(param, datetime.datetime):
                    paramstr += "'%s'," % param.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(param, datetime.date):
                    paramstr += "'%s'," % param.strftime('%Y-%m-%d')
                elif isinstance(param, list) or isinstance(param, dict):
                    paramstr += "'%s'," % DORM.escape_string_for_sql(JsonTool.to_json(param))
                elif isinstance(param, type(None)):
                    paramstr += "NULL,"
                elif isinstance(param, decimal.Decimal):
                    paramstr += "%s," % (float(param))
                else:
                    raise ValueError(f"不支持的值类型({type(param)}). field: {self.fields[index]}. value: {param}")
            valuestr += "(%s)," % paramstr.strip(",")
        colstr = colstr.strip(",")
        valuestr = valuestr.strip(",")
        self._sql_info["do"] = "INSERT INTO %s(%s) VALUES %s" % (self.table_name, colstr, valuestr)
        return self

    def update(self, **update_attrs):
        self.reset_sql_info()
        if update_attrs:
            # 此时self.__dict__就是条件
            udpatestr = self.get_update_str(update_attrs)
            self._sql_info["do"] = "UPDATE %s SET %s" % (self.table_name, udpatestr)
            self.set_where_str(self.__dict__, table_alias=self.table_alias)
        else:
            # 进入自动更新
            if not hasattr(self, "id"):
                raise ValueError("自动更新时必须有主键id！")
            # 处理mod_time
            if "mod_time" in self.__dict__:
                del self.__dict__["mod_time"]
            udpatestr = self.get_update_str(self.__dict__)
            self._sql_info["do"] = "UPDATE %s SET %s" % (self.table_name, udpatestr)
            self._sql_info["where"] = "WHERE id = %s" % self.id
        return self

    def where(self, **conditions):
        self.set_where_str(conditions, table_alias=self.table_alias)
        return self

    def where_condition_by_dict(self, conditions, specify_table=False):
        # specify_table 为 True, 则需要在字典的 key 中写表名或alias的名称 例如 {'user_info.id__in':[2,3]}
        self.set_where_str(conditions, specify_table=specify_table)
        return self

    def where_costomize_condition(self, where_str: str):
        if self.check_sql_is_insert():
            raise ValueError("INSERT不需要使用where子句！")
        # 定制where 条件 直接把定制的where条件 AND 到现有where中。
        if not self._sql_info["where"]:
            if where_str.strip().lower().startswith('and '):
                where_str = where_str.strip()[3:]
            self._sql_info["where"] = where_str.strip() if where_str.strip() == "" else "WHERE %s" % where_str
        else:
            # 说明已经有where了，补充条件
            self._sql_info["where"] += f" {where_str.strip()} " if where_str.strip() else ""

        return self

    def where_for_condition_and(self, **conditions):
        return self.where(**conditions)

    def where_for_condition_or(self, **conditions):
        self.set_where_str(conditions, table_alias=self.table_alias, condition_tag="OR")
        return self

    def group_by(self, group_by=""):
        if self.check_sql_is_select() is False:
            raise ValueError("只有SELECT的时候才可以使用group_by！")

        self._sql_info["group_by"] = "GROUP BY %s" % group_by if group_by else ""
        return self

    def order_by(self, order_by=""):
        if self.check_sql_is_select() is False:
            raise ValueError("只有SELECT的时候才可以使用order_by！")
        self._sql_info["order_by"] = "ORDER BY %s" % order_by if order_by else ""
        return self

    def limit(self, limit_1=0, limit_2=0):
        if limit_1 < 0:
            raise StandardError("limit1开始必须大于等于0")
        if limit_2 < 0:
            raise StandardError("limit2结束必须大于等于0")

        limit_str = ""
        if limit_2 != 0:
            # 有limit结束
            limit_str = f"LIMIT {limit_1},{limit_2}"
        else:
            # 没有limit结束
            if limit_1:
                limit_str = f"LIMIT {limit_1}"
            else:
                raise StandardError("limit开始和结束不能都是0")
        self._sql_info["limit"] = f"{limit_str}"
        return self

    def pagination(self, **kwargs):
        sql = self.get_sql()
        if not sql.lower().startswith("select "):
            raise ValueError("必须是查询语句才可以进行分页操作")
        return BaseOrm.pagination_sql(
            sql, **kwargs
        )

    def join(self, table_name, table_alias="", join_type="", join_on="", col_str="",
             query_where_condition_dict=None,
             query_where_condition_or_list=None):
        """
        连表查询
        :param table_name: 表名
        :param table_alias: 别名
        :param join_type: 枚举 left right inner outter
        :param join_on: join条件，就是 ON 后面的 ON x.id=x.id
        :param col_str: 要查询哪些列，例如查询bug的name和priority列就是  "bug.name, bug.priority as bug_priority"
        :param query_where_condition_dict: where查询条件，格式如下：
                {
                    "id":1, "name__like": "王"
                }
        :param query_where_condition_or_list: where的or的查询条件，是个列表，每个列表是一个字典，字典里面是一组or的查询，
                                                多个就是多个字段，举例如下：
              [
                {
                    "name__in": ["王吉亮", "李亚超"],
                    "org_id__in": [1, 2]
                },
                {
                    "name__in": ["仲晓明", "许丽军"],
                    "org_id__in": [4, 5]
                },
              ]
        :return:
        """

        if self.check_sql_is_select() is False:
            raise StandardError("只有调用了 query方法 后才可以使用 left_join")
        if not join_on:
            raise StandardError("join_on是必填参数")
        join_dict = {
            "table_name": table_name,
            "table_alias": table_alias,
            "join_type": join_type,
            "join_on": join_on,
            "col_str": col_str,
            "query_where_condition_dict": query_where_condition_dict,
            "query_where_condition_or_list": query_where_condition_or_list,
        }
        self._sql_info["join_list"].append(join_dict)
        return self

    def update_join_info(self, table_name, table_alias, join_type, join_on, query_condition_dict):
        key_dict = {'table_name': table_name, 'table_alias': table_alias}
        find_flag = False
        for item in self._sql_info['join_list']:
            if all([item[key] == val for key, val in key_dict.items()]):
                item['query_where_condition_dict'].update(query_condition_dict or {})
                find_flag = True

        if not find_flag:
            return self.join(table_name, table_alias, join_type, join_on,
                             query_where_condition_dict=query_condition_dict)
        return self

    def join_dict(self, **kwargs):
        default_dict = {
            'table_name': '',
            'table_alias': '',
            'join_type': '',
            'join_on': '',
            'col_str': '',
            'query_where_condition_dict': None,
            'query_where_condition_or_list': None
        }
        for item in default_dict.keys():
            if item in kwargs:
                default_dict[item] = kwargs[item]
        return self.join(**kwargs)

    def left_join(self, table_name, table_alias="", join_on="", col_str="", query_where_condition_dict=None,
                  query_where_condition_or_list=None):
        return self.join(table_name, table_alias=table_alias, join_type="left", join_on=join_on,
                         col_str=col_str, query_where_condition_dict=query_where_condition_dict,
                         query_where_condition_or_list=query_where_condition_or_list)

    def right_join(self, table_name, table_alias="", join_on="", col_str="", query_where_condition_dict=None,
                   query_where_condition_or_list=None):
        return self.join(table_name, table_alias=table_alias, join_type="right", join_on=join_on,
                         col_str=col_str, query_where_condition_dict=query_where_condition_dict,
                         query_where_condition_or_list=query_where_condition_or_list)

    def inner_join(self, table_name, table_alias="", join_on="", col_str="", query_where_condition_dict=None,
                   query_where_condition_or_list=None):
        return self.join(table_name, table_alias=table_alias, join_type="inner", join_on=join_on,
                         col_str=col_str, query_where_condition_dict=query_where_condition_dict,
                         query_where_condition_or_list=query_where_condition_or_list)

    def outer_join(self, table_name, table_alias="", join_on="", col_str="", query_where_condition_dict=None,
                   query_where_condition_or_list=None):
        return self.join(table_name, table_alias=table_alias, join_type="outer", join_on=join_on,
                         col_str=col_str, query_where_condition_dict=query_where_condition_dict,
                         query_where_condition_or_list=query_where_condition_or_list)

    def cross_join(self, table_name, table_alias="", join_on="", col_str="", query_where_condition_dict=None,
                   query_where_condition_or_list=None):
        return self.join(table_name, table_alias=table_alias, join_type="cross", join_on=join_on,
                         col_str=col_str, query_where_condition_dict=query_where_condition_dict,
                         query_where_condition_or_list=query_where_condition_or_list)

    def lock_row_for_update(self):
        if self.check_sql_is_select() is False:
            raise ValueError("只有SELECT的时候才可以使用lock_row_for_update！")
        # 执行此语句后，锁定更新，别人不可更新此条数据，但是可以查询。
        self._sql_info["lock"] = "for update"
        return self

    def lock_table(self):
        """ Lock table.

        Locks the object model table so that atomic update is possible.
        Simulatenous database access request pend until the lock is unlock()'ed.

        Note: If you need to lock multiple tables, you need to do lock them
        all in one SQL clause and this function is not enough. To avoid
        dead lock, all tables must be locked in the same order.

        See http://dev.mysql.com/doc/refman/5.0/en/lock-tables.html
        """
        cursor = DORM.connection.execute("LOCK TABLES %s WRITE" % self.validate_orderby_inject(self.table_name))
        row = cursor.fetchone()
        return row

    def unlock_table(self):
        """ Unlock the table. """
        cursor = DORM.connection.execute("UNLOCK TABLES")
        row = cursor.fetchone()
        return row

    @property
    def str_sql(self):
        return self.get_sql()

    def get_sql(self):
        if self._sql_info["do"].startswith("INSERT "):
            # 进入insert执行
            sql = self._sql_info["do"]

        elif self._sql_info["do"].startswith("SELECT "):
            if self.force_execute is False and self._sql_info["where"] == "":
                raise ValueError("SELECT 必须有where子句，否则请使用force_execute=True强制执行！")
            if self._sql_info["join_list"]:
                # 先获取查询列从SELECT 到 FROM
                base_select_columns_str = get_sub_string(self._sql_info["do"], "SELECT ", " FROM")
                all_select_columns_str = base_select_columns_str
                join_str = ""
                for tmp_join_dict in self._sql_info["join_list"]:
                    # 处理 查询 列 str
                    if tmp_join_dict.get("col_str"):
                        all_select_columns_str += f",{tmp_join_dict.get('col_str')}"
                    # 处理Join str
                    join_str += f'{tmp_join_dict.get("join_type").upper()} JOIN {tmp_join_dict.get("table_name")} ' \
                                f'{tmp_join_dict.get("table_alias")} ON {tmp_join_dict.get("join_on")} '
                    # 处理where
                    self.set_where_str(tmp_join_dict.get("query_where_condition_dict"),
                                       table_alias=tmp_join_dict.get("table_alias")
                                       if tmp_join_dict.get("table_alias")
                                       else tmp_join_dict.get("table_name"))
                    # 处理where的or
                    query_where_condition_or_list = tmp_join_dict.get("query_where_condition_or_list") \
                        if tmp_join_dict.get("query_where_condition_or_list") else []
                    for tmp_condition_or_dict in query_where_condition_or_list:
                        if not isinstance(tmp_condition_or_dict, dict):
                            # 不可以进行or查询
                            raise StandardError("连表查询时query_where_condition_or_list里面的元素必须是dict")
                        if len(tmp_condition_or_dict) < 2:
                            # 必须有两个key才可以
                            raise StandardError("连表查询时query_where_condition_or_list里面的dict至少2个key")
                        self.set_where_str(tmp_condition_or_dict,
                                           table_alias=tmp_join_dict.get("table_alias")
                                           if tmp_join_dict.get("table_alias")
                                           else tmp_join_dict.get("table_name"),
                                           condition_tag="OR")

                select_str = self._sql_info["do"].replace(base_select_columns_str, all_select_columns_str)
                sql = f'{select_str} {join_str} {self._sql_info["where"]} {self._sql_info["group_by"]} ' \
                      f'{self._sql_info["order_by"]} {self._sql_info["limit"]} {self._sql_info["lock"]}'
            else:
                sql = f'{self._sql_info["do"]} {self._sql_info["where"]} {self._sql_info["group_by"]} ' \
                      f'{self._sql_info["order_by"]} {self._sql_info["limit"]} {self._sql_info["lock"]}'

        elif self._sql_info["do"].startswith("DELETE "):
            if self.force_execute is False and self._sql_info["where"] == "":
                raise ValueError("DELETE 必须有where子句，否则请使用force_execute=True强制执行！")
            sql = f'{self._sql_info["do"]} {self._sql_info["where"]}'

        elif self._sql_info["do"].startswith("UPDATE "):
            if self.force_execute is False and self._sql_info["where"] == "":
                raise ValueError("UPDATE 必须有where子句，否则请使用force_execute=True强制执行！")
            sql = f'{self._sql_info["do"]} {self._sql_info["where"]}'

        else:
            raise ValueError("不合法的sql！")

        return sql

    def execute(self, sql=None, fetch="list", obj_type="orm", force_execute=False, **kwargs):
        """

        :param sql:
        :param fetch: list/one/first/last 默认list返回列表
        :param obj_type: 默认orm，返回dorm对象，可以传入dict，会把对象转为dict
        :param kwargs: execute_select_sql 或者 execute_update_sql 函数的可选参数
        :return:
        """
        if not sql:
            sql = self.get_sql()

        if sql.upper().startswith("SELECT "):
            all_data = DORM.execute_select_sql(sql, logger_errors=self.logger_errors,
                                               start_transaction=self.start_transaction, **kwargs)
            if fetch == "list":
                result = []
                for tmp_data_dict in all_data:
                    if obj_type == "orm":
                        tmporm = DORM(self.table_name, logger_errors=self.logger_errors,
                                      start_transaction=self.start_transaction)
                        tmporm.set_attrs(**tmp_data_dict)
                        result.append(tmporm)
                    elif obj_type == "dict":
                        result.append(tmp_data_dict)
                    else:
                        raise StandardError("错误的 obj_type，只能是 orm/dict。")
                return result
            else:
                if all_data:
                    if fetch == "first":
                        rowindex = 0
                    elif fetch == "last":
                        rowindex = -1
                    elif fetch == "one":
                        if len(all_data) == 1:
                            rowindex = 0
                        else:
                            raise StandardError("query fetch one时 返回多个值")
                    else:
                        raise StandardError("query fetch 只能是list/one/first/last")
                    row_dict = all_data[rowindex]
                    self.clear_attrs()
                    self.set_attrs(**row_dict)
                    if obj_type == "orm":
                        return self
                    elif obj_type == "dict":
                        return self.get_attrs()
                    else:
                        raise StandardError("错误的 obj_type，只能是 orm/dict。")
                else:
                    raise StandardError("query fetch 非list时没有查询到值")

        elif sql.upper().startswith("UPDATE "):
            return DORM.execute_update_sql(sql,
                                           logger_errors=self.logger_errors,
                                           start_transaction=self.start_transaction,
                                           log_result=True,
                                           **kwargs)
        elif sql.upper().startswith("DELETE "):
            return DORM.execute_update_sql(sql,
                                           logger_errors=self.logger_errors,
                                           start_transaction=self.start_transaction,
                                           log_result=True,
                                           **kwargs)
        elif sql.upper().startswith("INSERT "):
            retid = DORM.execute_update_sql(sql,
                                            logger_errors=self.logger_errors,
                                            start_transaction=self.start_transaction,
                                            log_result=True,
                                            **kwargs)
            setattr(self, "id", retid)
            return retid
        else:
            raise ValueError(f"不合法的sql语句：{sql}")

    def dict_query(self, sql=None, fetch="list", **kwargs):
        return self.execute(sql, fetch, obj_type='dict', **kwargs)

if __name__ == "__main__":
    pass
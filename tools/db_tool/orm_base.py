import datetime
import decimal
import math
import random
import re
import time
import traceback
import logging
import os
print(os.getenv('PYTHONPATH'))
from contextlib import ContextDecorator
from pymysql.err import IntegrityError
from tools.time_tool import get_current_time
from tools.util import md5
from tools.exception import StandardError
from tools.util import JsonTool
from tools.util import get_global_trace_id
from project import db

DEFAULT_DATE = "1970-01-02"
DEFAULT_TIME = "1970-01-02 00:00:00"


logging.basicConfig()
logger = logging.getLogger(__name__)


class Atomic(ContextDecorator):

    def __init__(self, **dorm_dict):
        self.db = db
        self.dorm_dict = dorm_dict

    def __enter__(self):
        for k, v in self.dorm_dict.items():
            v.start_transaction = True
        return self.dorm_dict

    def __exit__(self, exc_typ, exc_val, tb):
        if exc_typ:
            self.db.session.rollback()
        else:
            self.db.session.commit()


class BaseOrm(object):
    connection = db.session

    DEFAULT_DATE = DEFAULT_DATE
    DEFAULT_TIME = DEFAULT_TIME

    @staticmethod
    def atomic(**dorms):
        return Atomic(**dorms)

    # 静态公用方法
    @staticmethod
    def escape_string_for_sql(value):
        return str(value).replace('\\', '\\\\').replace('\0', '\\0').replace('\n', '\\n').replace('\r', '\\r') \
            .replace('\032', '\\Z').replace("'", "\\'").replace('"', '\\"').replace(':', '\\:')

    @staticmethod
    def escape_string_for_xss(value):
        return value.replace('<', '&lt;').replace('>', '&gt;')

    @staticmethod
    def escape_all_for_sql(value):
        return BaseOrm.escape_string_for_xss(BaseOrm.escape_string_for_sql(value))

    @staticmethod
    def validate_param_number(value):
        try:
            int(value)
            return value
        except:
            raise StandardError("ORM校验数字，值[%s]输入不合法！" % value)

    @staticmethod
    def validate_orderby_inject(value):
        # group by 的过滤也通用
        if isinstance(value, str):
            if re.match(".*(select|update|delete|insert|sleep|like|union|into|\(|\)|=|<|>|\*|\'|\"|\.\/).*", value, flags=re.IGNORECASE):
                raise StandardError("非法请求[%s]，请检查。" % value)
        return value

    @staticmethod
    def escape(value):
        if isinstance(value, int):
            return value
        elif isinstance(value, list):
            return [BaseOrm.escape(v) for v in value]
        elif isinstance(value, dict):
            return {BaseOrm.escape(k): BaseOrm.escape(v) for k, v in value.items()}
        else:
            return BaseOrm.escape_all_for_sql(value)

    @staticmethod
    def log_sql_result(sql_str, attr_dict, t_start, tsqlend, t_allend,
                       traceback_str, return_value, log_trace_id=None,
                       database=None, database_errmsg="", with_val=True):
        try:
            if log_trace_id is None:
                log_trace_id = get_global_trace_id()

            sql_elapsed_time = tsqlend - t_start
            all_elapsed_time = t_allend - t_start

            max_respones_size = int(1024 * 1024 * 1.5)
            # max_respones_size = 10
            if isinstance(return_value, list):
                output_return_value = []
                if return_value:
                    if len(JsonTool.to_json(return_value)) <= max_respones_size:
                        output_return_value = return_value
                    else:
                        output_return_value.append(return_value[0])
            else:
                return_value = str(return_value)
                output_return_value = return_value if len(return_value) <= max_respones_size \
                    else return_value[:max_respones_size]

            sql_res_dict = {
                "type": sql_str.strip().split(" ")[0].upper(),
                "elapsed_range": "",
                "sql_elapsed_time": "%.2f ms" % (sql_elapsed_time * 1000),
                "trace_id": log_trace_id,
                "sql_str": sql_str,
                "attr_dict": attr_dict,
                "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t_start)),
                "sql_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tsqlend)),
                "final_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t_allend)),
                "all_elapsed_time": "%.2f ms" % (all_elapsed_time * 1000),
                "exception": traceback_str if traceback_str else "",
                # "return_value": output_return_value,
                "result": "",
                "sql_elapsed_time_ms": sql_elapsed_time * 1000,
                "database": database,
                "database_errmsg": database_errmsg,
            }
            if with_val:
                sql_res_dict['return_value'] = output_return_value

            if traceback_str:
                sql_res_dict["result"] = "EXCEPTION"
                sql_res_dict["elapsed_range"] = "EEEEEE"
            else:
                if all_elapsed_time < 0.1:
                    sql_res_dict["result"] = "PASS"
                    sql_res_dict["elapsed_range"] = "< 100 ms"
                elif all_elapsed_time < 0.3:
                    sql_res_dict["result"] = "PASS"
                    sql_res_dict["elapsed_range"] = "< 300 ms"
                elif all_elapsed_time < 0.6:
                    sql_res_dict["result"] = "WARNING"
                    sql_res_dict["elapsed_range"] = "< 600 ms"
                elif all_elapsed_time < 1:
                    sql_res_dict["result"] = "WARNING"
                    sql_res_dict["elapsed_range"] = "< 1000 ms"
                elif all_elapsed_time < 2:
                    sql_res_dict["result"] = "WARNING"
                    sql_res_dict["elapsed_range"] = "< 2000 ms"
                elif all_elapsed_time < 3:
                    sql_res_dict["result"] = "ERROR"
                    sql_res_dict["elapsed_range"] = "< 3000 ms"
                else:
                    sql_res_dict["result"] = "ERROR"
                    sql_res_dict["elapsed_range"] = ">= 3000 ms"
            sql_res_dict_jsonstr = JsonTool.to_json(sql_res_dict)
            logger.info(sql_res_dict_jsonstr)

            # sql_res_dict["result"] = "ERROR"
            if sql_elapsed_time > 3:
                logger.warning(
                    "慢查询报警：SQL执行时间[%s]秒过长，详情：\n%s" % (int(sql_elapsed_time), sql_res_dict_jsonstr)
                )

        except:
            logger.error("execute_select_sql logging failed: %s" % traceback.format_exc())

    @staticmethod
    def execute_select_sql(sql_str, attr_dict=None, return_type='list', json_list_keys=None, json_dict_keys=None,
                           log_trace_id=None, logger_errors=True, use_connection=None, decimal_to_float=False,
                           start_transaction=False, log_result=False):
        """

        :param sql_str:
        :param return_type: list 就是返回列表，否则是返回 cursor指针，不耗费内存
        :param check_datetime:
        :param json_keys:
        :param json_default:
        :param json_list_keys:
        :param json_dict_keys:
        :param auto_json_loads:
        :param keys_func:
        :param log_trace_id:
        :param logger_errors:
        :param json_keys_ignore_errors:
        :param eval_keys:
        :param use_connection:
        :return:
        """
        t_start, tsqlend = 0, 0
        traceback_str = None
        return_value = None
        used_database = ""
        database_errmsg = ""
        try:
            if use_connection is None:
                # 写走主库，查走从库功能，如果上线有问题，迅速回滚到connection
                use_connection = BaseOrm.connection

            if not isinstance(attr_dict, dict):
                attr_dict = {}
            t_start = time.time()
            cursor_result = use_connection.execute(sql_str, attr_dict)
            tsqlend = time.time()

            if use_connection == BaseOrm.connection:
                used_database = "default"
            else:
                used_database = "unknown"

            if return_type != "list":
                return cursor_result

            # 如果是返回列表
            all_data = cursor_result.fetchall()
            col_names = list(cursor_result.keys())
            result = []
            for row_data in all_data:
                obj_dict = {}
                # 把每一行的数据遍历出来放到Dict中
                for index_colname in range(0, len(col_names)):
                    colname = col_names[index_colname]
                    rowvalue = row_data[index_colname]
                    # 处理 json_list_keys ，如果为空或者不合法，就是空列表
                    if isinstance(json_list_keys, list) and colname in json_list_keys:
                        if rowvalue:
                            rowvalue = JsonTool.to_dict_or_list(rowvalue)
                        else:
                            rowvalue = []

                    # 处理 json_dict_keys ，如果为空或者不合法，就是空字典
                    if isinstance(json_dict_keys, list) and colname in json_dict_keys:
                        if rowvalue:
                            rowvalue = JsonTool.to_dict_or_list(rowvalue)
                        else:
                            rowvalue = {}

                    # 处理默认时间，如果类型是data或者datetime，进行处理
                    if isinstance(rowvalue, datetime.datetime):
                        rowvalue_str = TimeTool.format_datetime_to_str(rowvalue)
                        if rowvalue_str == DEFAULT_TIME:
                            rowvalue = ""
                        else:
                            rowvalue = rowvalue_str

                    elif isinstance(rowvalue, datetime.date):
                        rowvalue_str = TimeTool.format_date_to_str(rowvalue)
                        if rowvalue_str == DEFAULT_DATE:
                            rowvalue = ""
                        else:
                            rowvalue = rowvalue_str

                    elif isinstance(rowvalue, str) and (colname.endswith("_date") or colname.endswith("_time")) and rowvalue.startswith("0000-00-00"):
                        rowvalue = ""

                    # 如果是decimal
                    if decimal_to_float and isinstance(rowvalue, decimal.Decimal):
                        rowvalue = float(rowvalue)

                    obj_dict[colname] = rowvalue
                result.append(obj_dict)

            # 返回列表的处理完毕
            return_value = result

            return result
        except Exception as e:
            if logger_errors:
                traceback_str = str(traceback.format_exc())
                logger.error("execute_select_sql:【异常_EXCEPTION_错误_ERROR】 | %s | attr_dict：(%s) | 异常信息: (%s)"
                                   % (sql_str, attr_dict, traceback.format_exc()))
            raise e
        finally:
            if not start_transaction:
                BaseOrm.connection.commit()
            # print("closed")
            BaseOrm.log_sql_result(sql_str, attr_dict,
                                   t_start, tsqlend, time.time(), traceback_str,
                                   return_value, log_trace_id=log_trace_id,
                                   database=used_database,
                                   database_errmsg=database_errmsg,
                                   with_val=log_result)

    @staticmethod
    def execute_update_sql(sql_str, attr_dict=None, logger_errors=True, log_trace_id=None, auto_commit=True,
                           start_transaction=False, log_result=False):
        t_start, tsqlend = 0, 0
        traceback_str = None
        return_value = -1
        used_database = "default"
        database_errmsg = ""

        try:
            if attr_dict is None:
                attr_dict = {}
            t_start = time.time()
            cursor_result = BaseOrm.connection.execute(sql_str, attr_dict)
            # 如果不在事务中，并且设置了自动提交，才会自动提交。
            if not start_transaction and auto_commit:
                BaseOrm.commit()

            tsqlend = time.time()
            if sql_str.strip().lower().startswith("insert "):
                # 如果是插入，返回主键id
                if cursor_result.rowcount == 1:
                    return_value = cursor_result.lastrowid
                    return return_value
                else:
                    return_value = cursor_result.rowcount
                    return return_value
            else:
                # 如果不是插入，返回影响行数
                return_value = cursor_result.rowcount
                return return_value
        except IntegrityError as e:
            if logger_errors:
                logger.error(f"execute_update_sql:【异常_EXCEPTION_错误_ERROR】 | {sql_str} "
                                      f"| attr_dict：({attr_dict}) "
                                      f"| 堆栈信息 : ({traceback.format_exc()}) "
                                      f"| 异常信息 : {e}")
        except Exception as e:
            database_errmsg = str(e)
            if logger_errors:
                traceback_str = str(traceback.format_exc())
                logger.error("execute_update_sql:【异常_EXCEPTION_错误_ERROR】 | %s | attr_dict：(%s) | 异常信息 : (%s)"
                                   % (sql_str, attr_dict, traceback.format_exc()))
            raise e
        finally:
            BaseOrm.log_sql_result(sql_str, attr_dict,
                                   t_start, tsqlend, time.time(), traceback_str,
                                   return_value, log_trace_id=log_trace_id,
                                   database=used_database,
                                   database_errmsg=database_errmsg,
                                   with_val=log_result)

    @staticmethod
    def commit():
        BaseOrm.connection.commit()

    @staticmethod
    def rollback():
        BaseOrm.connection.rollback()

    @staticmethod
    def get_select_sql_count(sql_str, attr_dict=None):
        if attr_dict is None:
            attr_dict = {}
        try:
            cursor = BaseOrm.connection.cursor()
            cursor.execute(sql_str, attr_dict)
        except Exception as opperr:
            cursor = BaseOrm.connection.cursor()
            cursor.execute(sql_str, attr_dict)
        return cursor.rowcount

    @staticmethod
    def truncate_table(table_name):
        try:
            cursor = BaseOrm.connection.cursor()
            return cursor.execute("TRUNCATE TABLE `%s`" % BaseOrm.validate_orderby_inject(table_name))
        except Exception as opperr:
            cursor = BaseOrm.connection.cursor()
            return cursor.execute("TRUNCATE TABLE `%s`" % BaseOrm.validate_orderby_inject(table_name))

    @staticmethod
    def pagination_sql(sql, page=1, limit=1, attr_dict=None, whether_groupby=False,
                       json_list_keys=None, json_dict_keys=None, log_trace_id=None, offset=None,
                       count_sql=None, count_derived_sql=None, use_connection=None, **kwargs):
        page, limit = int(page), int(limit)
        if not page:
            raise StandardError('page不能为0')

        if log_trace_id is None:
            log_trace_id = md5(get_current_time() + str(random.randint(0, 1000000000)))
        if attr_dict is None:
            attr_dict = {}

        # 获取总得data列表 获取countcou
        if count_derived_sql:
            count_sql = "SELECT COUNT(*) FROM ({}) tmp ".format(sql)
            whether_groupby = False

        if not count_sql:
            pattern = re.compile(r'^select\s(.*?)\sfrom\s', re.IGNORECASE)
            count_sql_str = re.sub(pattern, 'SELECT COUNT(*) FROM ', sql.replace("\n", "").strip())
        else:
            count_sql_str = count_sql

        if whether_groupby:
            cursor = BaseOrm.execute_select_sql(sql, attr_dict,
                                                return_type="cursor", log_trace_id=log_trace_id,
                                                use_connection=use_connection, **kwargs)
            all_data = cursor.fetchall()
            count_total_num = len(all_data)
            if count_total_num == 0:
                return {"datalist": [], "page": page, "limit": limit, "pagecount": 0, "totalcount": 0, 'offset': 0}
        else:
            exec_data = BaseOrm.execute_select_sql(count_sql_str.lower(), attr_dict,
                                                   log_trace_id=log_trace_id,
                                                   use_connection=use_connection, **kwargs)
            if not exec_data:
                return {"datalist": [], "page": page, "limit": limit, "pagecount": 0, "totalcount": 0, 'offset': 0}
            else:
                count_total_num = exec_data[0]['count(*)']

        # 分页处理
        sql += "  LIMIT :limit_first_start_index,:limit_second_page_items_count "
        page_count = math.ceil(count_total_num / limit)

        attr_dict["limit_first_start_index"] = (page * limit - limit) if offset is None else offset
        attr_dict["limit_second_page_items_count"] = limit

        # 获取分页查出的data列表
        page_data = BaseOrm.execute_select_sql(sql, attr_dict,
                                               json_list_keys=json_list_keys, json_dict_keys=json_dict_keys,
                                               log_trace_id=log_trace_id,
                                               use_connection=use_connection, **kwargs)

        return {
            "datalist": page_data,
            "page": page,
            "limit": limit,
            "pagecount": page_count,
            "totalcount": count_total_num,
            'offset': 0 if offset is None else (offset + len(page_data)),
        }

    @staticmethod
    def get_condition(condition_dict=None, key=None, table_alias="", col_name=None,
                      contype="IN-EQ", value_type="STR", sql_condition="AND", condition_value=None, ignore_zero=True, ignore_blank=True):
        """
        自动生成查询条件
        :param condition_dict: 条件dict
        :param key: 请求中所带的 key 值
        :param table_alias:  表 别名
        :param col_name: 列名
        :param contype: 条件类型，
               枚举值：
               IN-EQ    自动识别，如果list使用IN，如果一个元素使用=
               LIKE-OR  like查询并且OR链接 生成 sql_condition ( xxx LIKE '%Name%' OR xxx LIKE '%ddd%')
               LTE_FORDATE       小于日期
               GTE_FORDATE       大于日期
               LIKE              可以是某一列，传str，
                                 也可以是多列like并且OR连接，主要用户关键字匹配多列，table_alias/col_name要传str
               其他的可以是 > < = >= <= , 其他情况下就是根据contype当做判断符号加入
        :param value_type: 值类型
               NUM 数字类型，不用加引号
               STR 字符串类型，要加引号
        :return:
        """
        table_alias_dot = '.' if table_alias else ''

        value = condition_value or (condition_dict or {}).get(key)

        if ignore_zero:
            if not value:
                return ''
        else:
            if not value and value != 0:  # Sprint_id 为 0 时，指过滤需求池
                return ''

        if col_name is None:
            col_name = key
        col_name = f"`{col_name}`"
        if value_type == "NUM":
            value_quote = ""
        else:
            value_quote = "'"

        if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
            # 如果是str，切割
            statuslist = str(value).split(",")
        elif isinstance(value, list):
            # 如果是list 直接复制
            statuslist = value
        else:
            statuslist = []

        if contype.upper() == "IN-EQ":
            if len(statuslist) == 0:
                return ""
            elif len(statuslist) == 1:
                return " %s %s%s%s=%s%s%s " % (sql_condition, table_alias, table_alias_dot, col_name,
                                              value_quote,
                                              BaseOrm.escape_all_for_sql(str(statuslist[0]))
                                               if value_quote
                                               else BaseOrm.validate_param_number(str(statuslist[0])),
                                              value_quote)
            else:
                incondition = ""
                for tmpstatus in statuslist:
                    if not ignore_blank or str(tmpstatus).strip() != "":
                        incondition += "%s%s%s," % (value_quote,
                                                    BaseOrm.escape_all_for_sql(str(tmpstatus).strip())
                                                    if value_quote
                                                    else BaseOrm.validate_param_number(str(tmpstatus).strip()),
                                                    value_quote)
                incondition = incondition.strip(",")
                if incondition != "":
                    return " %s %s%s%s IN (%s) " % (sql_condition, table_alias, table_alias_dot, col_name, incondition)
        elif contype.upper() == "NOT-IN-EQ":
            if len(statuslist) == 0:
                return ""
            elif len(statuslist) == 1:
                return " %s %s%s%s!=%s%s%s " % (sql_condition, table_alias, table_alias_dot, col_name,
                                             value_quote,
                                             BaseOrm.escape_all_for_sql(str(statuslist[0]))
                                                if value_quote
                                                else BaseOrm.validate_param_number(str(statuslist[0])),
                                             value_quote)
            else:
                incondition = ""
                for tmpstatus in statuslist:
                    if str(tmpstatus).strip() != "":
                        incondition += "%s%s%s," % (value_quote,
                                                    BaseOrm.escape_all_for_sql(str(tmpstatus).strip())
                                                    if value_quote
                                                    else BaseOrm.validate_param_number(str(tmpstatus).strip()),
                                                    value_quote)
                incondition = incondition.strip(",")
                if incondition != "":
                    return " %s %s%s%s NOT IN (%s) " % (sql_condition, table_alias, table_alias_dot, col_name, incondition)
        elif contype.upper() == "LIKE-OR":
            if statuslist:
                tmpcondition = " %s (" % sql_condition
                for tmpfollower in statuslist:
                    tmpcondition += " %s%s%s LIKE '%%%s%%' OR" \
                                    % (table_alias, table_alias_dot,
                                       col_name,
                                       BaseOrm.escape_all_for_sql(str(tmpfollower)))
                tmpcondition = tmpcondition[:-2]
                tmpcondition += ") "
                return tmpcondition
        elif contype.upper() == "LIKE":
            if isinstance(col_name, list):
                tmpcondition = " %s (" % sql_condition
                for tmpcolindex in range(0, len(col_name)):
                    tmpcolname = col_name[tmpcolindex]
                    if isinstance(table_alias, list):
                        tmptablename = table_alias[tmpcolindex]
                    else:
                        tmptablename = str(table_alias)
                    tmpcondition += " %s%s%s LIKE '%%%s%%' OR" \
                                    % (tmptablename, table_alias_dot,
                                       tmpcolname,
                                       BaseOrm.escape_all_for_sql(str(value)))
                tmpcondition = tmpcondition[:-2]
                tmpcondition += ") "
                return tmpcondition
            elif isinstance(col_name, str):
                return " %s %s%s%s LIKE '%%%s%%' " % (sql_condition, table_alias, table_alias_dot, col_name,
                                                         BaseOrm.escape_all_for_sql(str(value)))
        elif contype.upper() == "LTE_FORDATE":
            dt = BaseOrm.escape_all_for_sql(str(value))
            if len(dt) == 10:
                return " %s %s%s%s<='%s 23:59:59' " % (sql_condition, table_alias, table_alias_dot, col_name, dt)
            else:
                return " %s %s%s%s<='%s' " % (sql_condition, table_alias, table_alias_dot, col_name, dt)
        elif contype.upper() == "GTE_FORDATE":
            dt = BaseOrm.escape_all_for_sql(str(value))
            if len(dt) == 10:
                return " %s %s%s%s>='%s 00:00:00' " % (sql_condition, table_alias, table_alias_dot, col_name, dt)
            else:
                return " %s %s%s%s>='%s' " % (sql_condition, table_alias, table_alias_dot, col_name, dt)
        else:
            return " %s %s%s%s %s %s%s%s " % (sql_condition, table_alias, table_alias_dot,
                                             col_name,
                                             contype,
                                             value_quote,
                                             BaseOrm.escape_all_for_sql(str(value))
                                              if value_quote
                                              else BaseOrm.validate_param_number(str(value)),
                                             value_quote)


    @staticmethod
    def batch_insert_sql(table_name, colname_list, value_list):
        value_len = len(colname_list)
        if not value_len:
            raise StandardError("必须传入至少1个列")
        if len(value_list) == 0:
            raise StandardError("必须有value")

        sql = "INSERT INTO `%s`(%s) VALUES" % (table_name, "`" + "`,`".join(colname_list) + "`")
        for i in range(0, len(value_list)):
            insert_values = value_list[i]
            if len(insert_values) != value_len:
                raise StandardError("第%s个元素长度与列数不符合" % (i+1))
            sub_insert_sql = "('" + "','".join([BaseOrm.escape_string_for_sql(str(x)) for x in insert_values]) + "'),"
            sql += sub_insert_sql
        sql = sql.strip(",")
        print(sql)
        return BaseOrm.execute_update_sql(sql_str=sql)


if __name__ == "__main__":
    res = BaseOrm.execute_select_sql("select * from users where id=46")
    print(res)

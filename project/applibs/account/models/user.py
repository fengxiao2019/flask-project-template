from tools.db_tool.orm import DORM

class User(object):
    T_NAME = "users"

    @staticmethod
    def query():
        query_obj = DORM(User.T_NAME).query('id').where(id=12)
        print(query_obj.str_sql)
        query_ans = query_obj.execute()
        for item in query_ans:
            print(item)

if __name__ == '__main__':
    User.query()

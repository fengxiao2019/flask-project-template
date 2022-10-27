from flask.cli import FlaskGroup
from tools.db_tool.orm import DORM


from project import app, db, redis_client
from tools.tablenames import Tablenames
from tools.time_tool import get_current_time


cli = FlaskGroup(app)

from project.register_bp import *

# 在引入create_all 前，需要先把model 引入
print(app.url_map)
@cli.command("create_db")
def create_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


@cli.command("check_redis")
def check_redis():
    redis_client.set('photo', 'ph')
    assert redis_client.get('photo') == 'ph'
    print('连接成功')


@cli.command('check_db')
def check_movie():
    DORM(Tablenames.favor).query('movie_id').where(movie_id__gt=0).dict_query()

@cli.command('create_mock_data')
def create_movie():
    batch_data = []
    for i in range(100):
        data =[ f'zhuangzhilingyun --{i}', get_current_time()]
        batch_data.append(data) 

    table = Tablenames.movie
    DORM(table).batch_insert(['title', 'add_time'], batch_data).execute()


@cli.command('create_mock_user')
def create_user():
    batch_data = []
    for i in range(100):
        data =[ f'shaoyz --{i}', get_current_time()]
        batch_data.append(data) 

    table = Tablenames.user
    DORM(table).batch_insert(['username', 'add_time'], batch_data).execute()



if __name__ == '__main__':
    cli()
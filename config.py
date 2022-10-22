import os

basedir = os.path.abspath(os.path.dirname(__file__))

from dotenv import load_dotenv
# load the environment


def get_app_env():
    flask_env = os.environ.get('FLASK_ENV', 'development')
    return flask_env


def get_app_env_file_name():
    return f'.{get_app_env()}.env'


# label the environment, development or production
cur_flask_env_label = get_app_env()

# return the environment filename
cur_flask_env = get_app_env_file_name()


ans = load_dotenv(os.path.join(basedir, f".env/{cur_flask_env}"))


class Config(object):
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite://")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.getenv('REDIS_URL')
    assert REDIS_URL is not None


class DevConfig(Config):
    DEBUG = True
    TESTING = True


class ProdConfig(Config):
    pass


_MAPPER_CONFIG = {
    'development': DevConfig,
    'production': ProdConfig
}


CUR_CONFIG = _MAPPER_CONFIG.get(cur_flask_env_label)

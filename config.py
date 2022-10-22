import os

basedir = os.path.abspath(os.path.dirname(__file__))

from dotenv import load_dotenv
# load the environment


load_dotenv(os.path.join(basedir, '.env'))


class Config(object):
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite://")
    print(SQLALCHEMY_DATABASE_URI)
    SQLALCHEMY_TRACK_MODIFICATIONS = False
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_redis import FlaskRedis
from tools.exception import ReturnDict


app = Flask(__name__)
app.config.from_object('config.CUR_CONFIG')

db = SQLAlchemy(app)
redis_client = FlaskRedis(app, decode_responses=True)
db.select

@app.route("/")
def hello_world():
    return jsonify(hello="world")

URL_VERSION = 'v1'
def json_response(code=ReturnDict.CODE_SUCCESS, msg='success', data={}):
    return ReturnDict(code=code, msg=msg, data=data).to_json()


def error_response(code=ReturnDict.CODE_EXCEPTION, msg='failed', data={}):
    return ReturnDict(code=code, msg=msg, data=data).to_json()

def dict_response(code=ReturnDict.CODE_SUCCESS, msg='success', data={}):
    return ReturnDict(code=code, msg=msg, data=data).to_dict()
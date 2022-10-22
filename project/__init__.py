from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_redis import FlaskRedis


app = Flask(__name__)
app.config.from_object('config.CUR_CONFIG')

db = SQLAlchemy(app) 
redis_client = FlaskRedis(app, decode_responses=True)


@app.route("/")
def hello_world():
    return jsonify(hello="world")
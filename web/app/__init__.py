import psycopg2
from config import Config
from confluent_kafka import Producer
from flask import Flask, current_app
from pymongo import MongoClient
from flask_cors import CORS



def create_app(config_class=Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)
    cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

 
    kafka_producer = Producer({"bootstrap.servers": app.config["KAFKA"]})
    mongo_db = MongoClient(f"mongodb://{app.config['MONGO_USER']}:{app.config['MONGO_PASSWORD']}@{app.config['MONGO_HOST']}:27017/?authSource=house_data")
    sql_db = psycopg2.connect(f"postgresql://{app.config['SQL_USER']}:{app.config['SQL_PASSWORD']}@{app.config['SQL_HOST']}:5432/house_data")
    with app.app_context():
        current_app.kafka_producer = kafka_producer
        current_app.mongo_db = mongo_db.house_data
        current_app.sql_db = sql_db
        

    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix="/api/v1")

    @app.route("/")
    def checker():
        return "UP"

    return app

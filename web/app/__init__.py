import psycopg2
from config import Config
from confluent_kafka import Producer
from flask import Flask, current_app
from pymongo import MongoClient
from flask_cors import CORS
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration


def create_app(config_class=Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)
    cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

    sentry_sdk.init(
        dsn="https://0cde96349a0145e485630af0f46c0f45@sentry.housestats.co.uk/4",
        integrations=[
            FlaskIntegration(),
        ],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0
    )

 
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

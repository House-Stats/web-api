import os

import psycopg2
import sentry_sdk
from config import Config
from flask import Flask, current_app
from flask_cors import CORS
from pymongo import MongoClient
from sentry_sdk.integrations.flask import FlaskIntegration
from celery import Celery  

def create_app(config_class=Config) -> Flask:
    app = Flask(__name__)
    config_object = config_class()
    app.config.from_object(config_object)
    celery = Celery('worker',
                    broker = config_object.CELERY_BROKER_URL,
                    backend = config_object.CELERY_RESULT_BACKEND
    )
    cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

    if not bool(config_object.manage_sensitive("DEBUG", "False")):
        sentry_sdk.init(
            dsn="https://463e69188a7e46fca4408d5f23284fe9@o4504585220980736.ingest.sentry.io/4504649931489280",
            integrations=[
                FlaskIntegration(),
            ],
            traces_sample_rate=1.0
        )

 
    mongo_db = MongoClient(f"mongodb://{app.config['MONGO_USER']}:{app.config['MONGO_PASSWORD']}@{app.config['MONGO_HOST']}:27017/?authSource=house_data")
    sql_db = psycopg2.connect(f"postgresql://{app.config['SQL_USER']}:{app.config['SQL_PASSWORD']}@{app.config['SQL_HOST']}:5432/house_data")
    with app.app_context():
        current_app.mongo_db = mongo_db.house_data
        current_app.sql_db = sql_db
        current_app.celery = celery
        

    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix="/api/v1")

    @app.route("/")
    def checker():
        return "UP"

    return app

import os

import psycopg2
import sentry_sdk
from config import Config
from flask import Flask, current_app
from flask_cors import CORS
from pymongo import MongoClient
from sentry_sdk.integrations.flask import FlaskIntegration


def create_app(config_class=Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)
    cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

    # if not bool(os.environ.get("DEBUG", False)):
    #     sentry_sdk.init(
    #         dsn="https://0cde96349a0145e485630af0f46c0f45@sentry.housestats.co.uk/4",
    #         integrations=[
    #             FlaskIntegration(),
    #         ],
    #         traces_sample_rate=1.0
    #     )

 
    mongo_db = MongoClient(f"mongodb://{app.config['MONGO_USER']}:{app.config['MONGO_PASSWORD']}@{app.config['MONGO_HOST']}:27017/?authSource=house_data")
    sql_db = psycopg2.connect(f"postgresql://{app.config['SQL_USER']}:{app.config['SQL_PASSWORD']}@{app.config['SQL_HOST']}:5432/house_data")
    with app.app_context():
        current_app.mongo_db = mongo_db.house_data
        current_app.sql_db = sql_db
        

    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix="/api/v1")

    @app.route("/")
    def checker():
        return "UP"

    return app

import os

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    SQL_USER = os.environ.get("POSTGRES_USER")
    SQL_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
    SQL_HOST = os.environ.get("POSTGRES_HOST")
    MONGO_HOST =os.environ.get("MONGO_HOST")
    MONGO_USER = os.environ.get("MONGO_USERNAME")
    MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND")

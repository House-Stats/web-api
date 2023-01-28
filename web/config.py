import os

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY')
    SQL_USER = os.environ.get("DB_USER")
    SQL_PASSWORD = os.environ.get("DB_PASSWORD")
    SQL_HOST = os.environ.get("DB_HOST")
    KAFKA = os.environ.get("KAFKA")
    MONGO_HOST =os.environ.get("MONGO_HOST")
    MONGO_USER = os.environ.get("MONGO_USER")
    MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
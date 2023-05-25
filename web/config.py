import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))

class Config():
    def __init__(self) -> None:
        load_dotenv()

        self.SECRET_KEY = self.manage_sensitive('SECRET_KEY')
        self.SQL_USER = self.manage_sensitive("POSTGRES_USER")
        self.SQL_PASSWORD = self.manage_sensitive("POSTGRES_PASSWORD")
        self.SQL_HOST = self.manage_sensitive("POSTGRES_HOST")
        self.MONGO_HOST = self.manage_sensitive("MONGO_HOST")
        self.MONGO_USER = self.manage_sensitive("MONGO_USERNAME")
        self.MONGO_PASSWORD = self.manage_sensitive("MONGO_PASSWORD")

    def manage_sensitive(self, name, default: str | None = None) -> str | None:
        v1 = os.environ.get(name)

        secret_fpath = f'/run/secrets/{name}'
        existence = os.path.exists(secret_fpath)

        if v1 is not None:
            return v1

        if existence:
            v2 = open(secret_fpath).read().rstrip('\n')
            return v2

        if all([v1 is None, not existence]) and default is None:
            raise KeyError(f'{name} environment variable is not defined')
        elif default is not None:
            return default

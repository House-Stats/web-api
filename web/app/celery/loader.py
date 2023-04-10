from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from typing import List, Tuple
import polars as pl


class Loader():
    def __init__(self, area: str, area_type: str, db_cur) -> None:
        self._cur = db_cur
        self.area_type = area_type.lower()
        self.area = area.upper()
        self._areas = ["postcode", "street", "town", "district", "county", "outcode", "area", "sector"]
        if self.area_type not in self._areas:
            raise ValueError("Invalid area type")
        else:
            if self.verify_area():
                data = self.fetch_area_sales()
                self.format_df(data)

    def verify_area(self):
        self._cur.execute(f"SELECT postcode FROM postcodes WHERE {self.area_type} = %s LIMIT 1;", (self.area,))
        if self._cur.fetchall() is not []:
            return True
        else:
            raise ValueError(f"Invalid {self.area_type} entered")

    def fetch_area_sales(self) -> List:
        query = f"""SELECT s.price, s.date, h.type, h.paon, h.saon, h.postcode, p.street, p.town, h.houseid
                FROM postcodes AS p
                INNER JOIN houses AS h ON p.postcode = h.postcode AND p.{self.area_type} = %s
                INNER JOIN sales AS s ON h.houseid = s.houseid AND h.type != 'O'
                WHERE s.ppd_cat = 'A' AND s.date < %s;
                """
        self._cur.execute(query, (self.area, self.latest_date))
        data = self._cur.fetchall()
        if data == []:
            raise ValueError(f"No sales for area {self.area}")
        else:
            return data

    def format_df(self, data: List[Tuple]):
        self._data = pl.DataFrame(data,
                                schema=["price","date","type","paon","saon",
                                        "postcode","street","town","houseid"],
                                orient="row")
        self._data = self._data.with_column(
            pl.col('date').apply(lambda x: datetime(*x.timetuple()[:-4])).alias("dt") # type: ignore
        )
        self._data = self._data \
            .drop("date") \
            .with_columns(pl.col("dt").alias("date")) \
            .drop("dt")

    def get_data(self) -> pl.DataFrame:
        return self._data

    @property
    def latest_date(self) -> datetime | None:
        self._cur.execute("SELECT data FROM settings WHERE name = 'last_updated';")
        latest_date = self._cur.fetchone()[0]
        if latest_date is not None:
            latest_date = datetime.fromtimestamp(float(latest_date))
            if latest_date > (datetime.now() - timedelta(days=60)):
                start = datetime.now().replace(day=1).replace(hour=0,minute=0,second=0, microsecond=0)
                return start - relativedelta(months=2)
            else:
                return latest_date

if __name__ == "__main__":
    from config import Config
    import psycopg2

    config = Config()
    conn = psycopg2.connect(f"postgresql://{config.SQL_USER}:{config.SQL_PASSWORD}@{config.SQL_HOST}:5432/house_data")
    lodr = Loader("CH2", "outcode", conn.cursor())
    print(lodr.latest_date)
from datetime import timedelta, datetime
from typing import Dict

from pymongo import MongoClient
import polars as pl
import psycopg2
from config import Config
from func_timer import Timer
from loader import Loader


class Analyse():
    def __init__(self):
        config = Config()
        self._sql_db = psycopg2.connect(f"postgresql://{config.SQL_USER}:{config.SQL_PASSWORD}@{config.SQL_HOST}:5432/house_data")
        self._mongo_db = MongoClient(f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:27017/?authSource=house_data")
        self._cur = self._sql_db.cursor()
        self._mongo = self._mongo_db.house_data

    def __del__(self):
        self._sql_db.close()
        self._mongo_db.close()

    def run(self, area: str, area_type: str):
        area = area.upper()
        area_type = area_type.upper()
        self.timer = Timer()

        self.timer.start("loader")
        try:
            self._loader = Loader(area, area_type, self._cur)
        except ValueError as e:
            return e
        self.timer.end("loader")

        self._data = self._loader.get_data()

        self.timer.start("aggregate")
        self._stats = self.get_all_data()
        self.timer.end("aggregate")

        timings = self.timer.get_times

        return_data = {
            "_id": area + area_type,
            "area": area,
            "area_type": area_type,
            "last_updated": datetime.now(),
            "timings": timings,
            "stats": self._stats
        }
        self._cache_results(return_data)

    @property
    def stats(self):
        return self._stats

    def _cache_results(self, return_data: Dict) -> None:
        area_record = self._mongo.cache.find_one({
            "_id": return_data["_id"]
        })
        if area_record is not None:
            self._mongo.cache.update_one(
                {"_id": return_data["_id"]},
                {"$set":{
                        "last_updated": return_data["last_updated"],
                        "timings": return_data["timings"],
                        "stats": return_data["stats"]
                }})
        else:
            self._mongo.cache.insert_one(return_data)

    def _get_average_prices(self) -> Dict:
        self.timer.start("aggregate_average")
        df = self._data.partition_by("type", as_dict=True)
        house_types_means = {}
        for house_type in df:
            average_prices = self._calc_average_price(df[house_type])
            house_types_means[house_type] = self.pad_df(average_prices).to_dict(as_series=False)

        house_types_means["all"] = self.pad_df(self._calc_average_price(self._data)).to_dict(as_series=False)
        data = {
            "type": [key for key in sorted(house_types_means)],
            "prices": [house_types_means[key]["price"] for key in sorted(house_types_means)],
            "dates": house_types_means["all"]["date"]
        }
        self.timer.end("aggregate_average")
        return data

    def _calc_average_price(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df \
            .sort("date") \
            .groupby_dynamic("date", every="1mo") \
            .agg(pl.col("price").log().mean().exp())
        return df

    def _get_type_proportions(self) -> Dict:
        self.timer.start("aggregate_proportions")
        df = self._data
        df = df \
            .unique(subset=["houseid"]) \
            .groupby("type").count()
        data = df.to_dict(as_series=False)
        self.timer.end("aggregate_proportions")
        return data

    def _get_monthly_qtys(self) -> Dict:
        self.timer.start("aggregate_qty")
        df = self._data.partition_by("type", as_dict=True)
        monthly_quantity = {}
        for house_type in df:
            volume = self._calc_monthly_qty(df[house_type])
            monthly_quantity[house_type] = self.pad_df(volume).to_dict(as_series=False)

        monthly_quantity["all"] = self.pad_df(self._calc_monthly_qty(self._data)).to_dict(as_series=False)

        data = {
            "type": [key for key in sorted(monthly_quantity)],
            "qty": [monthly_quantity[key]["qty"] for key in sorted(monthly_quantity)],
            "dates": monthly_quantity["all"]["date"]
        }
        self.timer.end("aggregate_qty")
        return data

    def _calc_monthly_qty(self, df: pl.DataFrame) -> pl.DataFrame:
        volume = df \
            .sort("date") \
            .groupby_dynamic("date", every="1mo") \
            .agg(pl.col("price").count().alias("qty"))
        return volume

    def _get_monthly_volumes(self) -> Dict:
        self.timer.start("aggregate_vol")
        df = self._data.partition_by("type", as_dict=True)
        monthly_volume = {}
        for house_type in df:
            volume = self._calc_monthly_vol(df[house_type])
            monthly_volume[house_type] = self.pad_df(volume).to_dict(as_series=False)

        monthly_volume["all"] = self.pad_df(self._calc_monthly_vol(self._data)).to_dict(as_series=False)

        data = {
            "type": [key for key in sorted(monthly_volume)],
            "volume": [monthly_volume[key]["volume"] for key in sorted(monthly_volume)],
            "dates": monthly_volume["all"]["date"]
        }
        self.timer.end("aggregate_vol")
        return data

    def _calc_monthly_vol(self, df: pl.DataFrame) -> pl.DataFrame:
        volume = df \
            .sort("date") \
            .groupby_dynamic("date", every="1mo") \
            .agg(pl.col("price").sum().alias("volume"))
        return volume

    def _get_percs(self) -> Dict:
        self.timer.start("aggregate_perc")
        data = self._data.partition_by("type", as_dict=True)
        monthly_perc = {}
        for house_type in data:
            monthly_perc[house_type] = self.pad_df(self._calc_ind_perc(data[house_type])).to_dict(as_series=False)

        monthly_perc["all"] = self.pad_df(self._calc_ind_perc(self._data)).to_dict(as_series=False)

        self.timer.end("aggregate_perc")
        return monthly_perc

    def _calc_ind_perc(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.sort("date") \
            .groupby_dynamic("date", every="1mo") \
            .agg(pl.col("price").log().mean().exp().alias("avg_price"))

        df = df.with_columns([
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.year().alias("year")
        ])

        df = df \
            .groupby("month") \
            .apply(self._calc_monthly_perc) \
            .drop(["year", "month", "prev_year", "avg_price"]) \
            .sort("date")
        return df

    def _calc_monthly_perc(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df \
            .sort("date") \
            .with_columns(
                pl.col("avg_price").shift().alias("prev_year")
            ) \
            .filter(pl.col("prev_year").is_not_null())

        df = df.with_columns(
            (((pl.col("avg_price")-pl.col("prev_year"))/pl.col("avg_price")*100)/12).alias("perc_change")
        )
        return df

    def _quick_stats(self, data) -> Dict[str, float]:

        current_month = data["average_price"]["dates"][-1]
        current_average = data["average_price"]["prices"][4][-1]
        prev_average = data["average_price"]["prices"][4][-2]
        average_change = round(100*(current_average-prev_average)/prev_average, 2)

        current_qty = data["monthly_qty"]["qty"][4][-1]
        prev_qty = data["monthly_qty"]["qty"][4][-2]
        qty_change = round(100*(current_qty-prev_qty)/prev_qty,2)

        current_vol =  data["monthly_volume"]["volume"][4][-1]
        prev_vol = data["monthly_volume"]["volume"][4][-2]
        vol_change = round(100*(current_vol-prev_vol)/prev_vol,2)

        try:
            expensive_sale = (self._data
                .filter(pl.col("date").is_between(current_month, current_month + timedelta(days=31)))
                .filter(pl.col("price") == pl.col("price").max())
                )[0,0]
        except:
            expensive_sale = 0

        quick_stats = {
            "current_month": current_month,
            "average_price": current_average,
            "average_change": average_change,
            "sales_qty": current_qty,
            "sales_qty_change": qty_change,
            "sales_volume": current_vol,
            "sales_volume_change": vol_change,
            "expensive_sale": expensive_sale
        }
        return quick_stats

    def get_all_data(self) -> Dict:
        data = {
            "average_price": self._get_average_prices(),
            "type_proportions": self._get_type_proportions(),
            "monthly_qty": self._get_monthly_qtys(),
            "monthly_volume": self._get_monthly_volumes(),
            "percentage_change": self._get_percs()
        }

        data["quick_stats"] = self._quick_stats(data)
        return data

    def pad_df(self, df: pl.DataFrame) -> pl.DataFrame | None:
        latest_date = self._loader.latest_date
        if latest_date is not None:
            dates = pl.date_range(datetime(1995,1,1), latest_date, "1mo")
            dates_df = pl.DataFrame(dates, schema=["date"])
            df = df.join(dates_df, on="date", how="outer")
            df = df.fill_null(0)
            return df


if __name__ == "__main__":
    task = Analyse()
    task.run("CH2 1", "sector")

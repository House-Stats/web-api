from typing import Tuple

import requests
from bs4 import BeautifulSoup
from config import Config
from pymongo import MongoClient


class GetEPC():
    def __init__(self) -> None:
        config = Config()
        self._mongo_db = MongoClient(f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:27017/?authSource=house_data")
        self._mongo = self._mongo_db.house_data

    def _get_houses(self, postcode: str) -> str:
        url_postcode = "+".join(postcode.split(" "))
        resp = requests.get(f"https://find-energy-certificate.service.gov.uk/find-a-certificate/search-by-postcode?postcode={url_postcode}")
        house_soup = BeautifulSoup(resp.content.decode("UTF-8"), 'html.parser')
        house_tags = house_soup.select("#main-content > div > div > table > tbody > tr")
        houses = []
        for house in house_tags:
            properties = house.find(name="th").find("a")
            address = properties.contents[0] \
                .replace("\n", "") \
                .strip() \
                .split(",")[0] \
                .upper()
            cert = properties["href"]
            houses.append((address, cert))
        return houses

    def get_cert(self, path: str):
        resp = requests.get(f"https://find-energy-certificate.service.gov.uk{path}")
        cert_soup = BeautifulSoup(resp.content.decode("UTF-8"), 'html.parser')
        sqr_m = cert_soup.select_one("#main-content > div > div.govuk-grid-column-two-thirds.epc-domestic-sections > div.govuk-body.epc-blue-bottom.printable-area.epc-box-container > dl > div:nth-child(2) > dd") \
            .contents[0] \
            .replace("\n", "") \
            .replace("square metres", "") \
            .strip()
        sqr_m = int(sqr_m)

        energy_rating = cert_soup.select_one("#epc > svg > svg.rating-current > text") \
            .contents[0] \
            .replace("|", "") \
            .split(" ")[0] \
            .strip()
        energy_rating = int(energy_rating)

        return (sqr_m, energy_rating)

    def run(self, postcode: str, paon: str, saon: str):
        houses = self._get_houses(postcode)
        if saon != "":
            house_id = f"{saon} {paon}".upper()
        else:
            house_id = paon.upper()
        try:
            house = list(filter(lambda x: x[0] == house_id,houses))[0]
        except IndexError:
            return {
                "sqr_m": None,
                "energy_rating": None,
                "cert_id":  None
            }
        cert_stats = self.get_cert(house[1])
        self._insert_data(cert_stats, house[1], postcode, paon, saon)
        return {
                "sqr_m": cert_stats[0],
                "energy_rating": cert_stats[1],
                "cert_id":  house[1]
            }

    def _insert_data(self, cert_stats: Tuple[int,int], cert_id: str, postcode: str, paon: str, saon: str ):
        epc_doc = self._mongo.epc_certs.find_one({"_id": f"{paon}{saon}{postcode}"})
        doc = {
                "_id": f"{paon}{saon}{postcode}",
                "sqr_m": cert_stats[0],
                "energy_rating": cert_stats[1],
                "cert_id": cert_id
            }
        if epc_doc is None:
            self._mongo.epc_certs.insert_one(doc)
        elif epc_doc != doc:
            self._mongo.epc_certs.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "sqr_m": cert_stats[0],
                    "energy_rating": cert_stats[1],
                    "cert_id": cert_id
                }}
            )



if __name__ == "__main__":
    cert = GetEPC()
    print(cert.run("CH64 1RG", "MEADOW VIEW", "2"))

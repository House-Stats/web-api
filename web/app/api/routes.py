import urllib.parse
from datetime import datetime
from pickle import dumps
from typing import List, Tuple

from app.api import bp
from flask import current_app, jsonify, url_for, abort, request


@bp.route("/analyse/<string:area_type>/<string:area>")
def index(area_type, area):
    data = dumps((area.upper(), area_type.upper()))
    with current_app.app_context():
        while True:
            try:
                print("Sending query")
                current_app.kafka_producer.produce("query_queue", data)  # Send each sale as string to kafka
                current_app.kafka_producer.poll(0)
                break
            except BufferError:
                current_app.kafka_producer.flush()
    query_id = (area + area_type).replace(" ", "").upper()
    return jsonify(
        status="ok",
        query_id=query_id,
        result=f"https://api.housestats.co.uk{url_for('api.fetch_results',query_id=query_id)}"
    )

@bp.route("/fetch/<string:query_id>")
def fetch_results(query_id):
    with current_app.app_context():
        query = current_app.mongo_db.cache.find_one({"_id": query_id.upper()})
        if query is not None:
            if query["last_updated"] > get_last_updated():
                return jsonify(
                    results=query,
                    outdated=False,
                    done=True
                    )
            else:
                return jsonify(
                    results=query,
                    outdated=True,
                    done=True
                )
        else:
            return jsonify(
                outdated=False,
                done=False
            )

@bp.route("/search/<string:query>")
def search_area(query):
    query = urllib.parse.unquote(query).upper()
    query_filter = request.args.get("filter")
    if query_filter is not None:
        if query_filter in ["postcode", "street", "town", "district", "county", "outcode", "area", "sector"]:
            sql_query = f"""SELECT area, area_type 
                        FROM areas WHERE substr(area, 1, 50) 
                        LIKE '{query}%' AND area_type = '{query_filter}'
                        ORDER BY char_length(area)
                        LIMIT 10;"""
            print(sql_query)
        else:
            return abort(404)
    else:
        sql_query = f"""SELECT area, area_type 
                   FROM areas WHERE substr(area, 1, 50) 
                   LIKE '{query}%' 
                   ORDER BY char_length(area)
                   LIMIT 10;"""
    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_query)
        results: List[Tuple[str,str]] = cur.fetchall()
    if len(results) > 0:
        SORT_ORDER = {"area": 1, "outcode": 0, "sector": 2, "postcode": 3, "town": 4, "county": 5, "district": 6, "street": 7}
        return_list = []
        for area in results:
            if area[1] not in ["postcode","outcode","sector","area"]:
                return_list.append((area[0].title(), area[1].title()))
            else:
                return_list.append((area[0], area[1].title()))
        return_list.sort(key=lambda val: SORT_ORDER[val[1].lower()], reverse=True)
        return jsonify(
            results=return_list,
            found=True
        )
    else:
        return jsonify(
            results=None,
            found=False
        )

@bp.route("/find/<string:postcode>")
def search_houses(postcode):
    sql_query = """SELECT h.type, h.paon, h.saon, h.postcode, p.street, p.town, p.county
                    FROM postcodes AS p
                    INNER JOIN houses AS h ON p.postcode = h.postcode AND p.postcode = %s;"""
    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_query, (postcode.upper(),))
        results: List[Tuple[str,str,str,str,str,str]] = cur.fetchall()
    results = sorted(list(set(results)), key=lambda x: x[1])
    if results != []:
        return jsonify(
            results=results,
        )
    else:
        return abort(404, "Cannot Find Houses for Postcode")

@bp.route("/find/<string:postcode>/<string:paon>")
def get_house(postcode, paon):
    sql_query = """SELECT h.type, h.paon, h.saon, h.postcode, p.street, p.town, s.tui, s.date, s.price, s.new, s.freehold, s.ppd_cat
                    FROM postcodes AS p
                    INNER JOIN houses AS h ON p.postcode = h.postcode AND p.postcode = %s AND h.paon = %s
                    INNER JOIN sales AS s ON h.houseid = s.houseid
                    ORDER BY s.date DESC;"""
    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_query, (postcode.upper(),paon.upper()))
        results: List[Tuple] = cur.fetchall()
    if results != []:
        return jsonify(
            results=results,
        )
    else:
        return abort(404, "No House Found")

@bp.route("/find/<string:postcode>/<string:paon>/<string:saon>")
def get_house_saon(postcode, paon, saon):
    sql_query = """SELECT h.type, h.paon, h.saon, h.postcode, p.street, p.town, s.tui, s.date, s.price, s.new, s.freehold, s.ppd_cat
                    FROM postcodes AS p
                    INNER JOIN houses AS h ON p.postcode = h.postcode AND p.postcode = %s AND h.paon = %s AND h.saon = %s
                    INNER JOIN sales AS s ON h.houseid = s.houseid
                    ORDER BY s.date DESC;"""
    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_query, (postcode.upper(),paon.upper(),saon.upper(),))
        results: List[Tuple] = cur.fetchall()
    if results != []:
        return jsonify(
            results=results,
        )
    else:
        return abort(404, "No House Found")

def get_last_updated():
    cur = current_app.sql_db.cursor()
    cur.execute("SELECT * FROM settings WHERE name = 'last_updated';")
    last_updated = cur.fetchone()
    if last_updated == None:
        return datetime.fromtimestamp(0)
    else:
        if last_updated[1] is not None:
            return datetime.fromtimestamp(float(last_updated[1]))
        else:
            return datetime.fromtimestamp(0)

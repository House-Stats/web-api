import urllib.parse
from datetime import datetime
from pickle import dumps
from typing import List, Tuple

from app.api import bp
from flask import current_app, jsonify, url_for, abort, request
from app.celery import analyse_task

@bp.route("/analyse/<string:area_type>/<string:area>")
def index(area_type, area):
    with current_app.app_context():
        query_id = area.upper() + area_type.upper()
        result = current_app.mongo_db.cache.find_one({"_id": query_id})
    if result is None:
        task = analyse_task.delay(area, area_type)
        return jsonify(
            status="ok",
            task_id=task.id,
            result=f"https://api.housestats.co.uk{url_for('api.fetch_results',query_id=query_id)}?task_id={task.id}"
        )
    else:
        return jsonify(
            status="ok",
            result=f"https://api.housestats.co.uk{url_for('api.fetch_results',query_id=query_id)}"
        )

@bp.route("/get/<string:query_id>")
def fetch_results(query_id):
    task_id = request.args.get("task_id", None)
    if task_id is not None:
        task = analyse_task.AsyncResult(task_id)
        if task.state == "PENDING":
            return {
                "status": task.state
            }
        else:
            query_id = task.wait()
            with current_app.app_context():
                result = current_app.mongo_db.cache.find_one({"_id": query_id})
            return {
                "status": task.state,
                "result": result
            }
    else:
        with current_app.app_context():
            result = current_app.mongo_db.cache.find_one({"_id": query_id})
        if result is not None:
            return {
                "status": "COMPLETED",
                "result": result
            }
        else:
            return {
                "status": "FAILED"
            }


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
        SORT_ORDER = {"area": 0, "outcode": 1, "sector": 2, "postcode": 3, "town": 4, "county": 5, "district": 6, "street": 7}
        return_list = []
        for area in results:
            if area[1] not in ["postcode","outcode","sector","area"]:
                return_list.append((area[0].title(), area[1].title()))
            else:
                return_list.append((area[0], area[1].title()))
        return_list.sort(key=lambda val: SORT_ORDER[val[1].lower()])
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
    sql_house_query = """SELECT h.houseid, h.type, h.paon, h.saon, h.postcode, p.street, p.town
                    FROM postcodes AS p
                    INNER JOIN houses AS h ON p.postcode = h.postcode AND p.postcode = %s 
                    WHERE h.paon = %s"""
    sql_sales_query = """SELECT *
                    FROM sales
                    WHERE houseid = %s
                    ORDER BY date DESC;"""
    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_house_query, (postcode.upper(),paon.upper(),)) # Gets house 
        house: List[Tuple] = cur.fetchone()
        if house != []:
            cur.execute(sql_sales_query, (house[0],)) # gets all sales for the house
            sales = cur.fetchall()
            house_info = {
                "paon": house[2],
                "saon": house[3],
                "postcode": house[4],
                "street": house[5],
                "town": house[6],
                "type": house[1],
                "sales": sales
            }
            return jsonify(house_info)
        else:
            return abort(404, "No House Found")

@bp.route("/find/<string:postcode>/<string:paon>/<string:saon>")
def get_house_saon(postcode, paon, saon):
    sql_house_query = """SELECT h.houseid, h.type, h.paon, h.saon, h.postcode, p.street, p.town
                    FROM postcodes AS p
                    INNER JOIN houses AS h ON p.postcode = h.postcode AND p.postcode = %s 
                    WHERE h.paon = %s AND h.saon = %s;"""
    sql_sales_query = """SELECT *
                    FROM sales
                    WHERE houseid = %s
                    ORDER BY date DESC;"""
    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_house_query, (postcode.upper(),paon.upper(),saon.upper(),)) # Gets house 
        house: List[Tuple] = cur.fetchone()
        if house != []:
            cur.execute(sql_sales_query, (house[0],)) # gets all sales for the house
            sales = cur.fetchall()
            house_info = {
                "paon": house[2],
                "saon": house[3],
                "postcode": house[4],
                "street": house[5],
                "town": house[6],
                "type": house[1],
                "sales": sales
            }
            return jsonify(house_info)
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

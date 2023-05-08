import urllib.parse
from datetime import datetime
from typing import List, Tuple

from app.api import bp, search_area_funcs
from app.celery import analyse_task, valuation_task
from flask import abort, current_app, jsonify, request, url_for
from app.api import epc_cert
from app.api import country

@bp.route("/analyse/<string:area_type>/<string:area>")
def index(area_type, area):
    with current_app.app_context():
        query_id = area.upper() + area_type.upper()
        result = current_app.mongo_db.cache.find_one({"_id": query_id})
    if result is None or result["last_updated"] < get_last_updated():
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
                return load_analysis(query_id, task_state=task.state)
    else:
        with current_app.app_context():
            return load_analysis(query_id)


@bp.route("/search/<string:query>")
def search_area(query):
    query = urllib.parse.unquote(query).upper()
    query_filter = request.args.get("filter", None)

    sql_query = search_area_funcs.generate_sql_query(query, query_filter=query_filter)
    if sql_query == "":
        return "Failed to generate query", 500

    with current_app.app_context():
        cur = current_app.sql_db.cursor()
        cur.execute(sql_query)
        results: List[Tuple[str,str]] = cur.fetchall()

    if len(results) > 0:
        sorted_res = search_area_funcs.sort_results(results)
        return jsonify(
            results=sorted_res,
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

@bp.route("/find/<string:postcode>/<path:house>")
def get_house_saon(postcode, house):
    try:
        paon, saon = house.split("/")
    except ValueError:
        paon = house
        saon = ""
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
            try:
                house_info["epc_cert"] = epc_cert.GetEPC().run(postcode, paon, saon)
            except:
                house_info["epc_cert"] =  {
                    "sqr_m": None,
                    "energy_rating": None,
                    "cert_id":  None
                }

            return jsonify(house_info)
        else:
            return abort(404, "No House Found")

@bp.route("/overview")
def overview():
    with current_app.app_context():
        data = load_analysis("OVERVIEW")["result"]
        if data is not None:
            data2 = load_analysis("ALLCOUNTRY")["result"]
            cur = current_app.sql_db.cursor()
            cur.execute("SELECT data FROM settings WHERE name = 'last_aggregated_counties'")
            last_update = cur.fetchone()
            if datetime.fromtimestamp(float(last_update[0])) < data["last_updated"] and datetime.fromtimestamp(float(last_update[0])) < data2["last_updated"]:
                return data
            else:
                data = country.get_overview(current_app)
                data["_id"] = "OVERVIEW"
                data["last_updated"] = datetime.now()
                current_app.mongo_db.cache.delete_one({"_id": "OVERVIEW"})
                current_app.mongo_db.cache.insert_one(data)
        else:
            data = country.get_overview(current_app)
            data["_id"] = "OVERVIEW"
            data["last_updated"] = datetime.now()
            current_app.mongo_db.cache.insert_one(data)
        return data

@bp.route("/value/calc/<string:houseid>")
def value_house(houseid: str):
    task = valuation_task.delay(houseid)
    return jsonify(
            status="ok",
            task_id="/value/get/" + task.id,
        )

@bp.route("/value/get/<string:job_id>")
def get_value(job_id: str):
    if job_id is not None:
        task = analyse_task.AsyncResult(job_id)
        if task.state == "PENDING":
            return {
                "status": task.state
            }
        else:
            valuations = task.wait()
            return {
                "valuations": valuations,
                "status": "ok"
            }

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

def load_analysis(query_id, task_state=None):
    result = current_app.mongo_db.cache.find_one({"_id": query_id})
    if result["last_updated"] > get_last_updated():
        try:
            result["stats"]["monthly_qty"]["type"].remove("all")
            result["stats"]["monthly_qty"]["qty"].pop(-1)
            result["stats"]["monthly_volume"]["type"].remove("all")
            result["stats"]["monthly_volume"]["volume"].pop(-1)
        except:
            pass
        return {
            "status": task_state if task_state is not None else "SUCCESS",
            "result": result
        }
    else:
        return {
            "status": task_state if task_state is not None else "FAILED"
        }
from flask import current_app

def check_status(current_app: current_app):
    pass

def get_overview(current_app: current_app):
    query = [
        {
            '$match': {
                'area_type': 'TOWN'
            }
        }, {
            '$project': {
                '3_month_perc': {
                    '$avg': {
                        '$slice': [
                            '$stats.percentage_change.all.perc_change', -3, 3
                        ]
                    }
                }
            }
        }, {
            '$sort': {
                '3_month_perc': -1
            }
        }, {
            '$limit': 5
        }
    ]
    top_5_towns = current_app.mongo_db.cache.aggregate(query)
    query[2]["$sort"]["3_month_perc"] = 1
    bottom_5_towns = current_app.mongo_db.cache.aggregate(query)
    country_data = current_app.mongo_db.cache.find_one({"_id": "ALLCOUNTRY"})
    return_data = country_data["stats"]
    return_data["timings"] = country_data["timings"]
    return_data["top_five"] = list(top_5_towns)
    return_data["bottom_five"] = list(bottom_5_towns)
    return return_data

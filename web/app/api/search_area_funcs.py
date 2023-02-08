import urllib.parse

def generate_sql_query(query: str, query_filter: str = None):
    if query_filter is not None:
        if query_filter in ["postcode", "street", "town", "district", "county", "outcode", "area", "sector"]:
            sql_query = f"""SELECT area, area_type 
                        FROM areas WHERE substr(area, 1, 50) 
                        LIKE '{query}%' AND area_type = '{query_filter}'
                        ORDER BY char_length(area)
                        LIMIT 10;"""
        else:
            return ""
    else:
        sql_query = f"""SELECT area, area_type 
                   FROM areas WHERE substr(area, 1, 50) 
                   LIKE '{query}%' 
                   ORDER BY char_length(area)
                   LIMIT 10;"""
    return sql_query

def sort_results(results):
    SORT_ORDER = {"area": 0, "outcode": 1, "sector": 2, "postcode": 3, "town": 4, "county": 5, "district": 6, "street": 7}
    return_list = []
    for area in results:
        if area[1] not in ["postcode","outcode","sector","area"]:
            return_list.append((area[0].title(), area[1].title()))
        else:
            return_list.append((area[0], area[1].title()))
    return_list.sort(key=lambda val: SORT_ORDER[val[1].lower()])
    return return_list
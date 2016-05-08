import sqlite3
from Redis import Redis
import CONFIG
from datetime import datetime
import rmq_negotiator

cachemanager = Redis()
USE_SQLITE = True

if USE_SQLITE:
    DBPATH = CONFIG.DBPATH

else:
    import pymysql as MySQLdb

db_user = ''
db_pass = ''
db_server = ''
db_name = ''
db_conn = db_server, db_user, db_pass, db_name,


def runQuery(query=tuple, connstring=db_conn):
    if not USE_SQLITE:
        db = MySQLdb.connect(
            host=connstring[0],
            user=connstring[1],
            passwd=connstring[2],
            db=connstring[3])
    else:
        db = sqlite3.connect(DBPATH)
    cursor = db.cursor()
    cursor.execute(query)
    response = cursor.fetchall()
    db.close()
    return response


def get_total_gender_for_year(year, gender, locale):
    sqlStr = """SELECT SUM(Count) FROM """
    if locale.upper() == 'US':
        sqlStr += " nationalnames "
        sqlStr += " WHERE Year=%s AND Gender='%s';" % (year, gender)
    else:
        sqlStr += " statenames "
        sqlStr += " WHERE year=%s AND Gender='%s' AND State='%s';" % (
            year, gender, locale)
    result = runQuery(query=sqlStr)[0][0]
    if result is None:
        result = 0
    return(result)


def get_name_data_in_region(name, gender, locale, first_year=1950):
    year_totals = {}
    for y in range(first_year, 2015):
        cache_key = gender + ':' + locale + ':' + str(y)
        total_gender_for_year = cachemanager.get_or_set(cache_key)
        if total_gender_for_year is None:
            total_gender_for_year = int(
                get_total_gender_for_year(y, gender, locale))
            cachemanager.get_or_set(cache_key, total_gender_for_year)
        year_totals[y] = int(total_gender_for_year)
    return get_name_data(name, gender, locale, year_totals, first_year)


def prepopulate_cache(year, gender, locale):
    cache_key = gender + ':' + locale + ':' + str(year)
    total_gender_for_year = cachemanager.get_or_set(cache_key)
    if total_gender_for_year is None:
        total_gender_for_year = int(
            get_total_gender_for_year(year, gender, locale))
        cachemanager.get_or_set(cache_key, total_gender_for_year)

    # print('Processing, %s : %s: %s please wait...' % (job['gender'], job['locale'], job['year']))
    print(cache_key, total_gender_for_year)
    return True

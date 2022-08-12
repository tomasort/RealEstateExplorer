import psycopg2
from pymongo import MongoClient
import datetime

conn = psycopg2.connect(host="localhost", port=5432, database="zillow", user="postgres", password="23943004")
cur = conn.cursor()
client = MongoClient()
db = client.zillow_final_test
zillow_collection = db.zillowCollection
sql = """INSERT INTO properties(id, price, city, zip, street_view, img_src, status, price_per_sqft, sold_price, latitude, 
                longitude, area, lot_size, address, detail_url, zestimate, price_change, price_reduction, rent_zestimate, unit, bathrooms, 
                date_price_changed, home_type, bedrooms, year_built, date_sold, home_status, tax_assessed_value) 
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
	"""
all_records = zillow_collection.find()
for record in all_records:
    if record['dateSold'] is not None:
        date_sold = datetime.datetime.utcfromtimestamp(record['dateSold']/1000)
        if record['dateSold'] == 0:
            date_sold = None
    if record['datePriceChanged'] is not None:
        price_change_date = datetime.datetime.utcfromtimestamp(record['datePriceChanged']/1000)
        if record['datePriceChanged'] == 0:
            price_change_date = None
    cur.execute(sql, (
        record['id'],
        record['price'] if type(record['price']) is float else -1.0,
        record['city'],
        record['zipcode'],
        record['streetViewURL'],
        record['imgSrc'],
        record['statusType'],
        record['pricePerSqft'],
        record['soldPrice'],
        record['latLong']['latitude'],
        record['latLong']['longitude'],
        record['area'],
        record['address'],
        record['detailUrl'],
        record['zestimate'],
        record['priceChange'],
        record['priceReduction'],
        record['rentZestimate'],
        record['unit'],
        record['bathrooms'],
        price_change_date if record['datePriceChanged'] is not None else record['datePriceChanged'],
        record['homeType'],
        record['bedrooms'],
        record['yearBuilt'],
        date_sold if record['dateSold'] is not None else record['dateSold'],
        record['homeStatus'],
        record['taxAssessedValue'] if record['taxAssessedValue'] != '' and type(record['taxAssessedValue']) == int else None,
    ))
    conn.commit()
    count = cur.rowcount
    print(count, "Record inserted successfully into mobile table")

cur.close()
conn.close()

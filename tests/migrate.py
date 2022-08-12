import mysql.connector
import psycopg2.extras
import datetime

conn = psycopg2.connect(host="localhost", port=5432,
                        database="zillow", user="tomasortega", password="23943004")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur.execute('SELECT * FROM properties')
all_records = cur.fetchall()

cnx = mysql.connector.connect(
    user='root', host='localhost', database='properties')
for values in all_records:
    cursor = cnx.cursor()
    row = {k: v if v is not None and v !=
                   '' else 'NULL' for k, v in values.items()}
    for k, v in row.items():
        if type(row[k]) == str and row[k] != 'NULL':
            row[k] = f"'{row[k]}'"
        if type(row[k]) == datetime.date:
            row[k] = f"'{str(row[k])}'"
    print(type(row['date_sold']))
    sql_statement = f"INSERT INTO properties(id,price,city,zip,status,home_type,bathrooms,bedrooms,street_view,img_src,price_per_sqft, " \
                    f"sold_price,latitude,longitude,area,lot_size,address,detail_url,zestimate,price_change,price_reduction,rent_zestimate," \
                    f"unit,date_price_changed,year_built,date_sold,home_status,tax_assessed_value) " \
                    f"VALUES({row['id']}, {row['price']}, {row['city']}, {row['zip']}, {row['status']}, {row['home_type']}, {row['bathrooms']}, " \
                    f"{row['bedrooms']}, {row['street_view']}, {row['img_src']}, {row['price_per_sqft']}, {row['sold_price']}, {row['latitude']}, " \
                    f"{row['longitude']}, {row['area']}, {row['lot_size']}, {row['address']}, {row['detail_url']}, {row['zestimate']}, " \
                    f"{row['price_change']}, {row['price_reduction']}, {row['rent_zestimate']}, {row['unit']}, CAST({row['date_price_changed']} as date), " \
                    f"{row['year_built']}, CAST({row['date_sold']} as date), {row['home_status']}, {row['tax_assessed_value']});"
    print(sql_statement)
    cursor.execute(sql_statement)
    cnx.commit()
    cursor.close()

cnx.close()

cur.close()
conn.close()

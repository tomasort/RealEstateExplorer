# TODO: Change all the instances of cur.execute() back to using %s and passing the values to the function execute() to avoid SQL injection
import json
import threading
import os
from os import listdir
from os.path import isfile, join
from datetime import date
from dotenv import load_dotenv
from decimal import Decimal
import pandas as pd
import mysql.connector
import requests
import datetime
from flask import Flask, request, render_template, session, flash, redirect, url_for

app = Flask(__name__)


DEV = bool(os.getenv('DEV'))
app.config.from_pyfile('config.py')
database_name = 'zillow'
user_name = 'tomasortega'
password = ''
host = 'localhost'
package_directory = os.path.dirname(os.path.abspath(__file__))
project_folder = os.path.expanduser('~/real_estate_project')
load_dotenv(os.path.join(project_folder, '.env'))



@app.route('/')
def home_page():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    return render_template('index.html', error_message='')

@app.route('/login', methods=['GET', 'POST'])
def login():
    password = os.getenv('MAIN_PASSWORD')
    if request.method == 'GET':
        return render_template('login.html')
    if request.form['password'] == password and request.form['username'] == 'admin':
        session['logged_in'] = True
    else:
        flash('wrong password!')
    return redirect(url_for('home_page'))


def get_properties_within(point, distance_range, connection, years=2):
    """ Returns all the properties that are within the specified distance

        arguments:

            point: The string representing the point at the center. the format is 'POINT(longitude latitude)'
                note: you need to reverse it for mysql 8 and leave it as is for 5.7

            distance_range: distance in meters from point

            connection: connection to the database

            years: The range of years to show

        returns: a list of dictionaries for all the properties that are within the range """

    results = []
    cur = connection.cursor(dictionary=True)
    try:
        sql = f"""
                SELECT address, price, (price / area) as price_per_sqft, tax_assessed_value, area, lot_size, home_type, status, date_sold, bathrooms, bedrooms, CAST(ST_Distance_Sphere(geog_point, ST_GeomFromText('{point}', 4326)) as unsigned) as distance, detail_url,  latitude, longitude
                FROM properties
                WHERE ST_Distance_Sphere(geog_point,ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')
                ORDER By distance
        """
        cur.execute(sql)
        sql = f"""
                SELECT count(id) 
                FROM properties
                WHERE ST_Distance_Sphere(geog_point,ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')
                ORDER By distance
        """
        print(sql)
        rows = cur.fetchall()
        for row in rows:
            result_row = {}
            for k, v in row.items():
                if k == 'lot_size' and v == -1:
                    v = None
                value = v if v is not None else ''
                if type(v) == Decimal:
                    value = float(v)
                elif type(v) == date:
                    value = str(v)
                result_row[k] = value
            results.append(result_row)
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cur.close()
    return results


def get_median(list_of_values):
    list_of_values = sorted(list_of_values)
    if len(list_of_values) % 2 == 0:
        first_value = list_of_values[len(list_of_values) // 2]
        second_values = list_of_values[(len(list_of_values) // 2) - 1]
        return (first_value + second_values) / 2
    else:
        return list_of_values[len(list_of_values) // 2]


def get_stats(point, distance_range, connection, years=2):
    """ Returns all the properties that are within the specified distance

        arguments:

            point: The string representing the point at the center. the format is 'POINT(longitude latitude)'
                note: you need to reverse it for mysql

            distance_range: distance in meters from point

            connection: connection to the database

            years: The range of years to show

        returns: a list of dictionaries for all the properties that are within the range """

    cur = connection.cursor(dictionary=True)
    stats = {}
    all_results = f"""SELECT price, area, lot_size, (price / area) as price_per_sqft FROM properties WHERE ST_Distance_Sphere(geog_point, ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')"""
    cur.execute(all_results)
    all_results = cur.fetchall()
    price_per_sqft = [round(float(row['price_per_sqft']), 2) for row in all_results if row['price_per_sqft'] is not None]
    prices = [round(float(row['price']), 2) for row in all_results if row['price'] is not None]
    areas = [round(float(row['area']), 2) for row in all_results if row['area'] is not None]
    lot_sizes = [round(float(row['lot_size']), 2) for row in all_results if row['lot_size'] is not None and row['lot_size'] != -1]
    #
    # Get the price statistics
    #
    # TODO: Find the first quartile and third quartile of price
    price_info_sql = f"""   SELECT avg(price) as average_price, min(price) as min_price, max(price) as max_price, STDDEV(price) as price_standard_deviation, 
                                avg(price / area) as price_per_sqft, avg(tax_assessed_value) as average_tax
                            FROM properties
                            WHERE ST_Distance_Sphere(geog_point, ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')"""
    cur.execute(price_info_sql)
    price_results = cur.fetchone()
    stats.update(price_results)
    stats['median_price'] = get_median(prices)
    #
    # Get the area statistics
    #
    price_info_sql = f"""   SELECT avg(area) as average_area, STDDEV(area) as area_standard_deviation, avg(lot_size) as average_lot_size
                            FROM properties
                            WHERE ST_Distance_Sphere(geog_point, ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')"""
    cur.execute(price_info_sql)
    area_results = cur.fetchone()
    stats.update(area_results)
    stats['median_price_per_square_feet'] = get_median(price_per_sqft)
    stats['median_area'] = get_median(areas)
    stats['median_lot_size'] = get_median(lot_sizes)
    #
    # Get the other statistics
    #
    sql = f""" 
            SELECT avg(bedrooms) as average_bedrooms, avg(bathrooms) as average_bathrooms, count(id) as number_of_properties
            FROM properties
            WHERE ST_Distance_Sphere(geog_point,ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')
        """
    cur.execute(sql)
    other_results = cur.fetchone()
    stats.update(other_results)
    sql = f""" 
            SELECT status, count(*) as number_of_properties
            FROM properties
            WHERE ST_Distance_Sphere(geog_point,ST_GeomFromText('{point}', 4326)) < {distance_range} AND (date_sold >= '{datetime.date.today().year - years}-01-01' OR status = 'FOR_SALE')
            GROUP BY status 
        """
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        stats[row['status']] = int(row['number_of_properties'])
    for k, v in stats.items():
        if type(v) == Decimal:
            v = float(v)
        stats[k] = v if v is not None else 'No Result'
    return stats


def get_percent_change(original_value, modified_value):
    dif = modified_value - original_value
    return (dif / float(original_value)) * 100


def get_target_info(price, area, stats):
    target_info = {}
    target_info['price'] = price
    target_info['area'] = area
    pc_median_price = get_percent_change(stats['median_price'], price)
    target_info[
        'median_price_message'] = f"Price is {abs(round(pc_median_price, 2))}% {'below' if pc_median_price < 0 else 'above'} the median"
    target_info[
        'std_deviations_price_from mean'] = f"Price is {round(abs(price - stats['median_price']) / stats['price_standard_deviation'], 2)} standard deviations from the mean"
    pc_average_price = get_percent_change(stats['average_price'], price)
    target_info[
        'average_price_message'] = f"Price is {abs(round(pc_average_price, 2))}% {'below' if pc_average_price < 0 else 'above'} the average"
    pc_median_area = get_percent_change(stats['median_area'], area)
    target_info[
        'median_area_message'] = f"Area is {abs(round(pc_median_area, 2))}% {'below' if pc_median_area < 0 else 'above'} the median"
    target_info[
        'std_deviations_area_from mean'] = f"Area is {round(abs(area - stats['median_area']) / stats['area_standard_deviation'], 2)} standard deviations from the mean"
    pc_average_area = get_percent_change(stats['average_area'], area)
    target_info[
        'average_area_message'] = f"Area is {abs(round(pc_average_area, 2))}% {'below' if pc_average_area < 0 else 'above'} the average"
    return target_info


def delete_old_files(path):
    # TODO: Change it so that if there are more than 10 you delete the last one!
    all_files = [f for f in listdir(path) if isfile(join(path, f))]
    files_to_remove = []
    for my_file in all_files:
        last_modified = os.stat(path + '/' + my_file).st_mtime
        last_modified_date = datetime.datetime.fromtimestamp(last_modified)
        delta_time = datetime.datetime.now() - last_modified_date
        if delta_time.seconds > 900:
            files_to_remove.append(path + '/' + my_file)
    for f in files_to_remove:
        os.remove(f)


@app.route('/', methods=['POST'])
def show_results():
    # TODO: put this input validation in the front end using js
    if not request.form['address']:
        return render_template('index.html', error_message='Enter a valid address')
    input_address = request.form['address'].strip()
    distance_range = request.form['range']
    if not distance_range.replace('.', '').isdigit():
        return render_template('index.html', error_message='Enter a valid distance range in Km')
    if not distance_range:
        distance_range = 0.5 * 1000
    else:
        distance_range = int(float(request.form['range']) * 1000)
    if float(request.form['range']) > 10:
        return render_template('index.html', error_message='The range must be less than 10km')
    try:
        if DEV:
            conn = mysql.connector.connect(database='properties', user='root')
        else:
            conn = mysql.connector.connect(host='tomasOrtega.mysql.pythonanywhere-services.com', database='tomasOrtega$properties', user='tomasOrtega', password=os.getenv('DB_PASSWORD'), )
        api_key = os.getenv('MAP_API_KEY')
        address = input_address.replace(' ', '+')
        address_json = json.loads(
            requests.get(f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}").text)
        longlat = address_json['results'][0]['geometry']['location']
        if DEV:
            point = f"POINT({longlat['lat']} {longlat['lng']})"
        else:
            point = f"POINT({longlat['lng']} {longlat['lat']})"
    except KeyError:
        return "<h1>Problem with the API key</h1>"
    except mysql.connector.Error as e:
        raise e
        return "<h1>Problem with the connection to the database! please try again later</h1>"
    except Exception:
        return "<h1>Problem with google's geolocation API</h1>"
    thread1 = threading.Thread(target=delete_old_files, args=(f"{package_directory}/static/files",))
    thread1.start()
    results = get_properties_within(point, distance_range, conn)
    stats = get_stats(point, distance_range, conn)
    target_info = {}
    if not results:
        return f"<h1>No Info Found on the Property at {input_address}<h1>"
    if request.form['target_area'] or request.form['target_price']:
        if request.form['target_price'].isdigit():
            price = int(request.form['target_price'])
        else:
            return render_template('index.html', error_message='The target Price must be an integer')
        if request.form['target_area'].isdigit():
            area = int(request.form['target_area'])
        else:
            return render_template('index.html', error_message='The target Area must be an integer')
        target_info = get_target_info(price, area, stats)
    else:
        target_info = {'No info about target': ''}
    file_path = f"{package_directory}/static/files/"
    map_url = f"https://maps.googleapis.com/maps/api/js?key={api_key}&callback=initMap"
    #
    # Save the results into csv
    #
    with open(f"{file_path}{abs(input_address.__hash__())}_results.csv", 'w') as my_file:
        if results:
            df = pd.DataFrame(results)
            df = df.drop(['longitude', 'latitude'], axis=1)
            df.to_csv(my_file)
    with open(f"{file_path}{abs(input_address.__hash__())}_stats.csv", 'w') as my_file:
        df = pd.DataFrame(stats, index=[0])
        df.to_csv(my_file)

    return render_template('results.html', results=json.dumps(results), lat=longlat['lat'], lng=longlat['lng'],
                           radius=distance_range, stats=stats, map_url=map_url, input_address=input_address,
                           target_info=target_info, file_name=f"files/{abs(input_address.__hash__())}_results.csv")


if __name__ == '__main__':
    app.run()

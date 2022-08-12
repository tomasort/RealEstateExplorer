import json
import re
import sys

import psycopg2
from bs4 import BeautifulSoup

sys.path.insert(0, '/Users/TomasOrtega/Projects/PropertyManager/src/scrapers')
from zillowscraper import ZillowScraper

PROXY_HOST = '5.79.73.131'
PROXY_PORT = '13010'
proxy = "http://" + PROXY_HOST + ":" + PROXY_PORT + "/"
zip_codes = {'33179', '33162', '33161', '33181', '33183', '33150', '33178', '33172', '33182', '33192',
             '33174', '33184', '33155', '33165', '33175', '33173', '33193', '33176', '33186', '33196', '33177'}
conn = psycopg2.connect(host="localhost", port=5432, database="zillow_test", user="postgres", password="23943004")
scraper = ZillowScraper(proxy=proxy, zip_codes=zip_codes, db_connection=conn, increment=50000, min_price=50000)
drop_table_sql = """DROP TABLE properties"""
create_table_sql = """
    CREATE TABLE properties(
    id varchar(15) PRIMARY KEY,
    price decimal(10, 3),
    city varchar(60),
    zip varchar(12),
    status varchar(12),
    home_Type varchar(20),
    bathrooms decimal(4, 1),
    bedrooms decimal(4, 1),
    street_view varchar(400),
    img_src varchar(400),
    price_Per_Sqft varchar(12),
    sold_price varchar(20),
    latitude numeric(10, 7),
    longitude numeric(10, 7),
    area integer,
    lot_size integer,
    address text,
    detail_url text,
    zestimate integer,
    price_change integer,
    price_reduction varchar(30),
    rent_zestimate integer,
    unit varchar(20),
    date_Price_Changed date,
    year_Built integer,
    date_Sold date,
    home_status varchar(20),
    tax_Assessed_Value decimal(10, 3)
);"""


def test_save_properties():
    cur = conn.cursor()
    try:
        cur.execute(create_table_sql)
        conn.commit()
        with open('/Users/TomasOrtega/Projects/PropertyManager/tests/properties_samples.json', 'r') as open_file:
            test_properties = json.load(open_file)
            num_of_properties = len(test_properties)
            for prop in test_properties:
                try:
                    scraper.save_properties(properties=[prop])
                except Exception as e:
                    raise e
            conn.commit()
            cur.execute('SELECT id FROM properties')
            assert cur.rowcount == num_of_properties
            for prop in test_properties:
                prop_id = prop['id']
                cur.execute('SELECT id FROM properties WHERE id = %s;', (prop_id,))
                for i in cur:
                    assert i[0] == prop_id
                if cur.rowcount == 0:
                    print("ID not found in the database")
                    assert False
    except Exception as e:
        raise e
    finally:
        cur.execute(drop_table_sql)
        conn.commit()
        cur.close()

#  TODO: Implement the tests for the remaining functions!

def test_is_captcha():
    with open('/Users/TomasOrtega/Projects/PropertyManager/tests/zillow_html/captcha_page.html', 'r') as captcha_page:
        soup = BeautifulSoup(captcha_page, features="lxml")
        assert scraper.is_captcha(soup)
    with open(
            '/Users/TomasOrtega/Projects/PropertyManager/tests/zillow_html/32145 Real Estate - 32145 Homes For Sale | Zillow.html',
            'r') as not_captcha_page:
        soup = BeautifulSoup(not_captcha_page, features="lxml")
        assert scraper.is_captcha(soup) is False


def test_parse_cookie():
    pass


def test_proccess_page():
    pass


def test_is_empty():
    pass


def test_get_number_of_properties():
    pass


def test_get_number_of_pages():
    pass


def test_handle_failed_pages():
    pass


def test_find_urls():
    pass


def test_fetch():
    urls = [
        'https://www.zillow.com/north-miami-beach-fl-33160/sold/?searchQueryState={%22pagination%22:{},%22usersSearchTerm%22:%2233160%22,%22mapBounds%22:{%22west%22:-80.18893820410153,%22east%22:-80.08319479589841,%22south%22:25.900151609879657,%22north%22:25.980730841994298},%22regionSelection%22:[{%22regionId%22:72463,%22regionType%22:7}],%22isMapVisible%22:true,%22mapZoom%22:13,%22filterState%22:{%22isRecentlySold%22:{%22value%22:true},%22isForSaleByAgent%22:{%22value%22:false},%22isForSaleByOwner%22:{%22value%22:false},%22isNewConstruction%22:{%22value%22:false},%22isComingSoon%22:{%22value%22:false},%22isAuction%22:{%22value%22:false},%22isForSaleForeclosure%22:{%22value%22:false},%22isPreMarketForeclosure%22:{%22value%22:false},%22isPreMarketPreForeclosure%22:{%22value%22:false},%22isMakeMeMove%22:{%22value%22:false}},%22isListVisible%22:true}',
        'https://www.zillow.com/north-miami-beach-fl-33160/?searchQueryState={%22pagination%22:{},%22usersSearchTerm%22:%2233160%22,%22mapBounds%22:{%22west%22:-80.16391855841061,%22east%22:-80.10821444158933,%22south%22:25.89667712833516,%22north%22:25.984202844986626},%22regionSelection%22:[{%22regionId%22:72463,%22regionType%22:7}],%22isMapVisible%22:true,%22mapZoom%22:14,%22filterState%22:{},%22isListVisible%22:true}',
        'https://www.zillow.com/', 'https://www.zillow.com/homes/miami_rb/',
        'https://www.zillow.com/homes/new-york_rb/', 'https://www.zillow.com/homes/32145_rb/',
        'https://www.zillow.com/homes/33311_rb/']

    titles = ['Recently Sold Homes in 33160 - [0-9],[0-9][0-9][0-9] Transactions | Zillow',
              '33160 Real Estate - 33160 Homes For Sale | Zillow',
              'Zillow: Real Estate, Apartments, Mortgages & Home Values',
              'Miami Real Estate - Miami FL Homes For Sale | Zillow',
              'NY Real Estate - New York Homes For Sale | Zillow',
              '32145 Real Estate - [0-9][0-9][0-9][0-9][0-9] Homes For Sale | Zillow',
              '33311 Real Estate - [0-9][0-9][0-9][0-9][0-9] Homes For Sale | Zillow']

    for i in range(len(urls) - 1):
        soup = scraper.fetch(urls[i])
        assert re.match(titles[i], soup.title.text)

# if __name__ == "__main__":
#     test_save_properties()

import psycopg2

from src.scrapers.zillowscraper import ZillowScraper

PROXY_HOST = '5.79.66.2'
PROXY_PORT = '13010'
proxy = "http://" + PROXY_HOST + ":" + PROXY_PORT + "/"
cities = {}
zip_codes = {'33019', '33020', '33021', '33023', '33024', '33025', '33026', '33027', '33028', '33029',
             '33146', '33150', '33155', '33161', '33162', '33165', '33166', '33169', '33172', '33173',
             '33174', '33175', '33176', '33177', '33178', '33179', '33181', '33182', '33183', '33184',
             '33186', '33193', '33196', '33312', '33068', '33063', '33311', '33162', '33405', '33406',
             '33426', '33021', '33460', '33460', '33324', '33441', '33435', '33444', '33461', '33404',
             '33334', '33322'}
zip_codes = {'33305'}
conn = psycopg2.connect(host="localhost", port=5432, database="zillow", user="tomasortega", password="23943004")
scraper = ZillowScraper(proxy=proxy, zip_codes=zip_codes, db_connection=conn, increment=30000, min_price=50000,
                        max_price=900000)
try:
    scraper.start_scraper()
except Exception as e:
    raise e
finally:
    pass

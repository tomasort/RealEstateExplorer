import datetime
import json
import psycopg2
from psycopg2 import errors
import threading
from src.scrapers.scraper import Scraper, MaxTriesError
from src.scrapers.zillowquery import ZillowQuery


class ZillowScraper(Scraper):
    def __init__(self, zip_codes, db_connection, proxy=None, max_price=800000, min_price=0, increment=100000,
                 max_tries=50, use_cookies=False, max_retries=20, num_of_threads=10):
        self.zip_codes = set(zip_codes)
        self.empty_pages = set()
        self.zillow_conn = db_connection
        self.zillow_cur = db_connection.cursor()
        self.max_price = max_price
        self.increment = increment
        self.min_price = min_price
        self.num_of_properties = 0
        self.scraped_zip_codes = set()
        self.num_of_threads = num_of_threads
        Scraper.__init__(self, proxy=proxy, max_tries=max_tries, use_cookies=use_cookies, max_retries=max_retries,
                         domain='www.zillow.com')

    def is_captcha(self, html_soup):
        if html_soup.title is None:
            return True
        return False

    def parse_cookie(self, response):
        # TODO: implement the parse_cookie method to be able to send cookies. Get the set cookie from response and add it to the set self.cookies
        pass

    def save_properties(self, properties=None):
        if properties is None or properties == []:
            return
        insert_property_sql = """INSERT INTO properties(id, price, city, zip, street_view, img_src, status, price_per_sqft, sold_price, latitude, 
                longitude, area, lot_size, address, detail_url, zestimate, price_change, price_reduction, rent_zestimate, unit, bathrooms, 
                date_price_changed, home_type, bedrooms, year_built, date_sold, home_status, tax_assessed_value) 
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        for prop in properties:
            if 'dateSold' in prop['hdpData']['homeInfo'].keys() and prop['hdpData']['homeInfo']['dateSold'] is not None:
                date_sold = datetime.datetime.utcfromtimestamp(prop['hdpData']['homeInfo']['dateSold'] / 1000)
                if prop['hdpData']['homeInfo']['dateSold'] == 0:
                    date_sold = None
            if 'datePriceChanged' in prop['hdpData']['homeInfo'].keys() and prop['hdpData']['homeInfo'][
                'datePriceChanged'] is not None:
                price_change_date = datetime.datetime.utcfromtimestamp(
                    prop['hdpData']['homeInfo']['datePriceChanged'] / 1000)
                if prop['hdpData']['homeInfo']['datePriceChanged'] == 0:
                    price_change_date = None
            tax_value = None if 'taxAssessedValue' not in prop['hdpData']['homeInfo'].keys() else \
                prop['hdpData']['homeInfo']['taxAssessedValue']
            values = (prop['id'],
                      prop['price'] if 'price' not in prop['hdpData']['homeInfo'].keys() else
                      prop['hdpData']['homeInfo']['price'],
                      None if 'city' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo']['city'],
                      None if 'zipcode' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'zipcode'],
                      None if 'streetViewURL' not in prop.keys() else prop['streetViewURL'],
                      None if 'imgSrc' not in prop.keys() else prop['imgSrc'],
                      None if 'statusType' not in prop.keys() else prop['statusType'],
                      None if 'pricePerSqft' not in prop.keys() else prop['pricePerSqft'],
                      None if 'soldPrice' not in prop.keys() else prop['soldPrice'],
                      None if 'latLong' not in prop.keys() else prop['latLong']['latitude'],
                      None if 'latLong' not in prop.keys() else prop['latLong']['longitude'],
                      None if 'area' not in prop.keys() else prop['area'],
                      None if 'lotSize' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'lotSize'],
                      None if 'address' not in prop.keys() else prop['address'],
                      None if 'detailUrl' not in prop.keys() else prop['detailUrl'],
                      None if 'zestimate' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'zestimate'],
                      None if 'priceChange' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'priceChange'],
                      None if 'priceReduction' not in prop['hdpData']['homeInfo'].keys() else
                      prop['hdpData']['homeInfo']['priceReduction'],
                      None if 'rentZestimate' not in prop['hdpData']['homeInfo'].keys() else
                      prop['hdpData']['homeInfo']['rentZestimate'],
                      None if 'unit' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo']['unit'],
                      None if 'bathrooms' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'bathrooms'],
                      price_change_date if 'datePriceChanged' in prop['hdpData']['homeInfo'].keys() else None,
                      None if 'homeType' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'homeType'],
                      None if 'bedrooms' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'bedrooms'],
                      None if 'yearBuilt' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'yearBuilt'],
                      date_sold if prop['hdpData']['homeInfo']['dateSold'] is not None else prop['hdpData']['homeInfo'][
                          'dateSold'],
                      None if 'homeStatus' not in prop['hdpData']['homeInfo'].keys() else prop['hdpData']['homeInfo'][
                          'homeStatus'],
                      tax_value if tax_value != '' and type(tax_value) == float else None)
            try:
                self.zillow_cur.execute(insert_property_sql, values)
                self.zillow_conn.commit()
            except errors.UniqueViolation as e:
                print(e)
                self.zillow_conn.rollback()
            except Exception as error:
                print(error)
                self.zillow_conn.rollback()

    def get_parsed_query_string(self, soup):
        query_state = soup.find('script', {'data-zrr-shared-data-key': 'mobileSearchPageStore'})
        assert query_state
        query_state = query_state.get_text().replace("<!--", "").replace("-->", "")
        parsed_query_state = json.loads(query_state)
        return parsed_query_state

    def get_base_url(self, soup, zip_code):
        pagination_tag = soup.find(class_='zsg-pagination_active')
        base_one, base_two = None, None
        if pagination_tag:
            base_one = pagination_tag.find('a').get('href').split('/')[1]
            base_one = f"/{base_one}/"
        conn = psycopg2.connect(host="localhost", port=5432, database="zipcodes", user="tomasortega",
                                password="23943004")
        try:
            cur = conn.cursor()
            sql = """SELECT city FROM zipcodes WHERE zip_code = %s"""
            cur.execute(sql, (zip_code,))
            if cur.rowcount > 0:
                base_two = "".join(['/', cur.fetchone()[0].lower().replace(' ', '-'), '-fl', f"-{zip_code}", '/'])
        except Exception as e:
            raise e
        finally:
            cur.close()
            conn.close()
        if base_one and base_two and (base_one != base_two):
            print(f"The bases are different! from pagination:{base_one}, from database: {base_two}")
            return base_one
        elif base_one is None and base_two:
            return base_two
        return base_one

    def process_page(self, html_soup, url):
        print(f"Processing page: {url}")
        try:
            parsed_query_string = self.get_parsed_query_string(html_soup)
            results = parsed_query_string['searchResults']['listResults']
            self.save_properties(results)
        except AssertionError:
            print("Can't find query state object in response")

    def is_empty(self, page_soup):
        zero_result_message = page_soup.find(class_='zero-results-message')
        if zero_result_message or page_soup.title is None:
            return True
        return False

    def get_number_of_properties(self, page_soup):
        num = page_soup.find(class_='result-count')
        if num:
            num = num.get_text().replace(',', '').replace('results', '')
        try:
            num = int(num)
        except (ValueError, TypeError):
            return 500
        return num

    def get_number_of_pages(self, page_soup):
        largest_num = 0  # The largest number that appears in the pagination section
        ordered_list_of_numbers = page_soup.find(class_='zsg-pagination')
        if ordered_list_of_numbers is None:
            return 0  # There is no pagination section in the page
        # Find the largest number in the list
        for list_item in ordered_list_of_numbers.children:
            if list_item.get_text().isnumeric():
                item = int(list_item.get_text())
                largest_num = item if item > largest_num else largest_num
        return largest_num

    def find_urls(self, zip_code):
        """ Make a list of all the urls necessary to scrap the given zip code """
        urls = set()
        zip_url = f"https://www.zillow.com/homes/{zip_code}_rb"
        try:
            main_url_soup = self.fetch(
                zip_url)  # initial page with the zip code to find the map settings and other info
        except MaxTriesError as e:
            print(e)
            return set()

        try:
            # Now parse the query state string from the response
            parsed_query_string = self.get_parsed_query_string(main_url_soup)
            base = self.get_base_url(main_url_soup, zip_code)
        except AssertionError:
            print("Unable to find the query string! going to try again later!", end='\r')
            return set()
        query_state_dict = parsed_query_string['queryState']
        for sold in [True, False]:
            current_max_price = self.min_price + self.increment
            while current_max_price <= self.max_price:
                query = ZillowQuery(min_price=current_max_price - self.increment, max_price=current_max_price,
                                    sold=sold, base=base, **query_state_dict)
                page_one_url = query.get_first_url()
                try:
                    page_one_soup = self.fetch(page_one_url)
                except MaxTriesError as e:
                    print(e)
                    current_max_price += self.increment
                    continue
                num_results = self.get_number_of_properties(page_one_soup)
                self.num_of_properties += num_results
                num_of_pages = self.get_number_of_pages(page_one_soup)
                if self.is_empty(page_one_soup):
                    self.empty_pages.add(page_one_url)
                    current_max_price += self.increment
                    continue
                self.process_page(page_one_soup, page_one_url)
                if num_of_pages != 0:
                    query_urls = query.get_urls(first=2, last=num_of_pages)
                    urls.update(query_urls)
                current_max_price += self.increment
        return urls

    def start_scraper(self):

        def process_zip(zip_code):
            print(f"Processing zip code: {zip_code}")
            urls = self.find_urls(zip_code)
            print(f"Processing Pages!")
            for url in urls:
                try:
                    self.process_page(self.fetch(url), url)
                except MaxTriesError:
                    continue
            print("handling failed pages:")
            self.handle_failed_pages()
            self.scraped_zip_codes.add(zip_code)

        while self.zip_codes:
            threads = []
            # self.num_of_threads = 1
            for _ in range(self.num_of_threads):
                if self.zip_codes:
                    current_zip_code = self.zip_codes.pop()
                    t = threading.Thread(target=process_zip, args=[current_zip_code])
                    t.start()
                    threads.append(t)

            for thread in threads:
                thread.join()

    def handle_failed_pages(self):
        failed_urls = set(self.failed_urls.keys())
        if not failed_urls:
            return
        while failed_urls:
            current_url = failed_urls.pop()
            if current_url in self.visited_sites:
                continue
            if 'home' in current_url:
                zip_code = current_url.split('/')[-1].replace('_rb', '')
                failed_urls.update(self.find_urls(zip_code))
            elif '_p' in current_url:
                try:
                    page_soup = self.fetch(current_url)
                except MaxTriesError:
                    continue
                if self.is_empty(page_soup):
                    self.empty_pages.add(current_url)
                    continue
                self.process_page(page_soup, current_url)
            self.failed_urls.pop(current_url)

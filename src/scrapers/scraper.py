import json
from random import choice, randint, randrange
import os
import socket
from bs4 import BeautifulSoup
from concurrent.futures import TimeoutError
from abc import abstractmethod, ABC
from urllib3 import ProxyManager, PoolManager, disable_warnings, exceptions
from collections import Counter, namedtuple
from urllib3.exceptions import MaxRetryError
from time import sleep
import signal

package_directory = os.path.dirname(os.path.abspath(__file__))


def _wait(tries):
    if tries == 2:
        sleep(randrange(2))
    if tries == 10:
        sleep(5)
    if tries == 20:
        sleep(10)
    if tries % 5 == 0:
        sleep(randrange(4))
    if tries % 11 == 0:
        sleep(randrange(3))
    if tries % 7 == 0:
        sleep(randrange(4))
    if tries % 13 == 0:
        sleep(randrange(3))
    if tries % 23 == 0:
        sleep(randrange(2))


class MaxTriesError(Exception):
    pass


class Scraper(ABC):

    def __init__(self, proxy=None, slow_crawl=True, max_tries=100, user_agents=None, referrers=None, use_cookies=False,
                 domain=None, max_retries=20, timeout=20):
        self.domain = domain
        self.user_agents = None
        if not user_agents:
            try:
                ua_file = open(os.path.join(package_directory, 'data', 'user_agents.json'))
                self.user_agents = json.load(ua_file)
                ua_file.close()
            except Exception as e:
                print("Couldn't load user agents, file not found!")
        if proxy:
            self.http = ProxyManager(proxy)
        else:
            disable_warnings(exceptions.InsecureRequestWarning)
            self.http = PoolManager()
        self.referrers = None
        if not referrers:
            try:
                referrers_file = open(os.path.join(package_directory, 'data', 'popular_sites.json'))
                self.referrers = json.load(referrers_file)
                referrers_file.close()
            except:
                print("Couldn't load referrers, file not found!")
        self.visited_sites = set()
        self.timeout_time = timeout
        self.max_tries = max_tries
        self.failed_urls = Counter()
        self.fetches = 0
        self.max_retries = max_retries
        self.slow_crawl = slow_crawl
        self.cookies = set()
        self.use_cookies = use_cookies

    def create_headers(self):
        headers = {'Accept-Language': choice(
            ["en-US,en;q=0.9,es;q=0.8", "en-US", "en-US,en;q=0.9", "*", "*;q=0.5", "en-US,en;q=0.5"]),
            'Accept-Encoding': choice(
                ["gzip, deflate, br", "gzip", "br", "gzip, br", "gzip, deflate", "deflate, gzip;q=1.0, *;q=0.5"]),
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Accept': choice([
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "text/html", "text/html,application/xhtml+xml",
                "text/html,application/xhtml+xml,application/xml;q=0.9",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng",
                "*/*",
                "text/plain; q=0.5, text/html, text/x-dvi; q=0.8, text/x-c",
                "text/*, text/plain, text/plain;format=flowed, */*",
                "text/*;q=0.3, text/html;q=0.7, text/html;level=1, text/html;level=2;q=0.4, */*;q=0.5"]),
            'Upgrade': "1", 'Sec-Fetch-User': "?1", 'Sec-Fetch-Site': "cross-site", 'Referer': choice(self.referrers)}
        if self.use_cookies and randint(0, 100) < 30:
            cookie = self.cookies.pop()
            raw_cookie = "".join(list(cookie))
            headers['Cookie'] = raw_cookie
        _key = choice(list(self.user_agents.keys()))  # get a random operating system to use as user agent
        headers['User-Agent'] = choice(self.user_agents[_key])
        if randint(0, 100) <= 20:
            headers['DNT'] = "1"
        headers['Referer'] = choice(self.referrers)
        if self.domain:
            headers['Host'] = self.domain
        return headers

    @abstractmethod
    def is_captcha(self, html_soup):
        pass

    @abstractmethod
    def parse_cookie(self, response) -> namedtuple:
        pass

    def fetch(self, url):
        soup, tries = None, 1
        while True:
            headers = self.create_headers()
            try:
                source = self.http.request('GET', url, headers=headers, timeout=10.0)
                self.fetches += 1
                soup = BeautifulSoup(source.data, 'lxml')
                if not self.is_captcha(soup):
                    # The page is valid so we can just return a soup version of the site
                    self.visited_sites.add(url)
                    if self.use_cookies:
                        # Add cookie to the pool of cookies
                        new_cookie = self.parse_cookie(source)
                        self.cookies.add(new_cookie)
                    break
                tries += 1
                if tries > self.max_tries:
                    print(f"The upper bound of tries has been reached for url {url}")
                    raise MaxTriesError
            except (MaxTriesError, TimeoutError, MaxRetryError, socket.timeout):
                if self.failed_urls[url] >= self.max_retries:
                    del self.failed_urls[url]
                    print("The URL failed too many times, check the proxy or the internet connection")
                else:
                    self.failed_urls.update([url])
                    print("Will try again")
                return BeautifulSoup("", 'lxml')
            if self.slow_crawl:
                _wait(tries)
        return soup



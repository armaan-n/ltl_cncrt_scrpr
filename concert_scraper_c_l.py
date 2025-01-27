import datetime
import random
import sys
from time import sleep

import psutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import re
import threading
from selenium.webdriver.support.ui import WebDriverWait
import boto3
import os
from selenium.webdriver.support import expected_conditions as EC

ips = ['3.220.167.184',
       '3.236.168.117',
       '44.197.132.90',
       '44.198.56.216',
       '44.201.22.245',
       '54.160.10.205',
       ]

threads = []
init_time = datetime.datetime.now()
init_time = str(init_time)
rand_number = random.random()
init_time += str(rand_number)

while '.' in init_time:
    init_time = init_time.replace('.', '-')

while ':' in init_time:
    init_time = init_time.replace(':', '-')

master_set = pd.DataFrame({
    'concert': [],
    'start_date': [],
    'end_date': [],
    'bands': [],
    'venue': [],
    'city': [],
    'state': [],
    'country': [],
    'setlist': [],
    'band_ids': []
})

sets_lock = threading.Lock()

my_ip = ''

wait_time = 30


class TimeoutHandler:
    def __init__(self, seconds, driver):
        self.seconds = seconds
        self.timer = None
        self.driver = driver

    def __enter__(self):
        self.start_timer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_timer()

    def start_timer(self):
        self.timer = threading.Timer(self.seconds, self.force_quit)
        self.timer.start()

    def stop_timer(self):
        if self.timer:
            self.timer.cancel()

    def force_quit(self):
        print("Force quitting due to timeout...")
        if self.driver:
            try:
                # Get the process ID
                process = psutil.Process(self.driver.service.process.pid)

                # Kill all chrome processes started by this script
                for child in process.children(recursive=True):
                    try:
                        child.kill()
                    except:
                        pass

                process.kill()
            except:
                pass


def get_new_ip():
    global my_ip
    my_ip = ips[random.randrange(0, len(ips))]


def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.page_load_strategy = 'none'

    service = Service('chromedriver-linux64/chromedriver-linux64/chromedriver')

    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(wait_time)
    driver.set_page_load_timeout(wait_time)

    #stealth(
    #    driver,
    #    languages=["en-US", "en"],
    #    vendor="Google Inc.",
    #    platform="Win32",
    #    webgl_vendor="Intel Inc.",
    #    renderer="Intel Iris OpenGL Engine",
    #    fix_hairline=True,
    #)

    return driver


def safe_get(thread_id, driver, wait, link, field):
    tries = 1
    my_wait_time = 1
    print(f'getting {link}')

    while True:
        timeout_handler = TimeoutHandler(wait_time, driver)

        try:
            with timeout_handler:
                get_new_ip()
                alt_link = link.replace('www.concertarchives.org', my_ip)

                for_ips = [
                    '3.236.168.117',
                    '44.197.132.90',
                    '44.201.22.245',
                    '3.220.167.184',
                    '54.160.10.205',
                    '52.207.69.203',
                    '44.198.56.216'
                ]

                for ip in for_ips:
                    alt_link = alt_link.replace(ip, my_ip)

                driver.get(alt_link)
                sleep(my_wait_time)
                wait.until(EC.visibility_of_element_located(
                    (By.CLASS_NAME, field)))
                break
        except Exception as e:
            print(f'thread {thread_id}: failed waiting {link}', flush=True)
            tries += 1

        if tries == 4:
            os.execv(sys.argv[0], sys.argv)

        driver = create_driver()
        wait = WebDriverWait(driver, wait_time)

    return driver, wait


client = boto3.client('sqs')
s3 = boto3.client('s3')


class ConcertScraper:
    def __init__(self):
        pass

    def scrape(self, thread_id):
        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        while True:
            start_dates = []
            end_dates = []
            concert_names = []
            bands = []
            venues = []
            cities = []
            states = []
            countries = []
            links = []
            band_ids = []
            setlists = []

            response = client.receive_message(
                QueueUrl=os.getenv('AWS_QUEUE_PATH', 'NA'),
                MaxNumberOfMessages=1,
                WaitTimeSeconds=0,
                VisibilityTimeout=900,
                MessageSystemAttributeNames=['All']
            )

            if 'Messages' not in response:
                break

            receive_count = int(response['Messages'][0]['Attributes']['ApproximateReceiveCount'])

            city, state, country, link = response['Messages'][0]['Body'].split(',')
            receipt_handle = response['Messages'][0]['ReceiptHandle']

            #if receive_count > 5:
            #    client.delete_message(QueueUrl=os.getenv('AWS_QUEUE_PATH', 'NA'),
            #                          ReceiptHandle=receipt_handle)
            #    continue

            driver, wait = safe_get(thread_id, driver, wait, link, 'table-responsive')

            concert_list = driver.find_elements(by=By.TAG_NAME, value='tbody')
            concert_list_elems = concert_list[0].find_elements(by=By.TAG_NAME,
                                                               value='tr')
            try:
                concert_list_elems += concert_list[1].find_elements(by=By.TAG_NAME, value='tr')
            except Exception as e:
                pass

            for concert_elem in concert_list_elems:
                concert_elems = concert_elem.find_elements(by=By.TAG_NAME,
                                                           value='td')

                concert_name, start_date, end_date, conc_bands, venue, _, _, _, link = self.scrape_concerts(
                    concert_elems, driver)
                concert_names.append(concert_name)
                start_dates.append(start_date)
                end_dates.append(end_date)
                venues.append(venue)
                cities.append(city)
                states.append(state)
                countries.append(country)
                links.append(link)

            for link in links:
                safe_get(thread_id, driver, wait, link, 'main-bnr')

                driver.implicitly_wait(0)
                list_elems = driver.find_elements(by=By.XPATH, value="//dl[@class='dl-horizontal']//dd//ol//li")
                band_list_elem = driver.find_element(by=By.CLASS_NAME, value='concert-band-list').find_elements(by=By.TAG_NAME, value='a')
                driver.implicitly_wait(wait_time)

                setlist = self.scrape_setlist(list_elems)
                conc_bands = self.scrape_bands(band_list_elem)
                conc_band_ids = self.scrape_band_ids(band_list_elem)

                setlists.append(setlist)
                bands.append(conc_bands)
                band_ids.append(conc_band_ids)

            setlist_strings = list(map(lambda l: ';'.join(l), setlists))
            band_strings = list(map(lambda l: ';'.join(l), bands))
            band_id_strings = list(map(lambda l: ';'.join(l), band_ids))

            mini_concert_set = pd.DataFrame({
                'concert': concert_names,
                'start_date': start_dates,
                'end_date': end_dates,
                'bands': band_strings,
                'venue': venues,
                'city': cities,
                'state': states,
                'country': countries,
                'setlist': setlist_strings,
                'band_ids': band_id_strings
            })

            client.delete_message(QueueUrl=os.getenv('AWS_QUEUE_PATH', 'NA'),
                                  ReceiptHandle=receipt_handle)

            with sets_lock:
                global master_set
                master_set = pd.concat([master_set, mini_concert_set])
                master_set.to_csv(f'./concert_set_{init_time}.csv', index=False)
                s3.upload_file(f'./concert_set_{init_time}.csv', 'concertbucket777', f'concert_set_{init_time}.csv')

    def scrape_setlist(self, list_elems):
        sets = []

        for elem in list_elems:
            sets.append(elem.text)

        return sets

    def scrape_bands(self, band_list_elem):
        bands = []

        for elem in band_list_elem:
            bands.append(elem.text)

        return bands

    def scrape_band_ids(self, band_list_elem):
        bands_ids = []

        for elem in band_list_elem:
            bands_ids.append(elem.get_attribute('href').split('/')[-1])

        return bands_ids

    def scrape_concert_name(self, concert_elems):
        concert_name = concert_elems[1].find_element(by=By.TAG_NAME,
                                                     value='a').text

        return concert_name

    def scrape_concert_date(self, concert_elems):
        date = concert_elems[0].text

        if '–' in date:
            split_date = date.split('–\n')

            start_date = split_date[0][:12]
            end_date = split_date[1][:12]
        else:
            start_date = date[:12]
            end_date = date[:12]

        return start_date, end_date

    def scrape_concert_link(self, concert_elems):
        concert_link = concert_elems[1].find_element(by=By.TAG_NAME, value='a').get_attribute('href')

        return concert_link

    def scrape_concert_bands(self, concert_elems, concert_name, driver):
        driver.implicitly_wait(0)

        bands_elem = concert_elems[1].find_elements(by=By.CLASS_NAME, value='concert-index-band-list')

        driver.implicitly_wait(wait_time)

        if len(bands_elem) != 0:
            band_names = bands_elem[0].text
        else:
            band_names = concert_name

        split_bands = re.split(' / | and |, ', band_names)
        joined_bands = ';'.join(split_bands)

        return joined_bands

    def scrape_concert_venue(self, concert_elems):
        venue = concert_elems[2].text

        return venue

    def split_location(self, loc):
        locs = loc.split(', ')

        if len(locs) == 3:
            city = locs[0]
            state = locs[1]
            country = locs[2]
        else:
            city = ''
            state = ''
            country = ''

        return city, state, country

    def scrape_concert_location(self, concert_elems):
        location = concert_elems[3].text

        return self.split_location(location)

    def scrape_concerts(self, concert_elems, driver):
        concert_name = self.scrape_concert_name(concert_elems)
        start_date, end_date = self.scrape_concert_date(concert_elems)
        band = self.scrape_concert_bands(concert_elems, concert_name, driver)
        venue = self.scrape_concert_venue(concert_elems)
        city, state, country = self.scrape_concert_location(concert_elems)
        link = self.scrape_concert_link(concert_elems)

        return concert_name, start_date, end_date, band, venue, city, state, country, link


if __name__ == "__main__":
    while True:
        try:
            concert_scraper = ConcertScraper()
            threads = []
            get_new_ip()
            concert_scraper.scrape(1)

        except Exception as e:
            pass

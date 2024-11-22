import datetime
import random
from time import sleep

import psutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import re
import threading
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3
import os

client = boto3.client('sqs')
s3 = boto3.client('s3')

threads = []
init_time = datetime.datetime.now()
init_time = str(init_time)
rand_number = random.random()
init_time += str(rand_number)

while '.' in init_time:
    init_time = init_time.replace('.', '-')

while ':' in init_time:
    init_time = init_time.replace(':', '-')

master_set = pd.DataFrame({'artist': [],
                           'genres': [],
                           'bio': [],
                           'artist_id': []})

sets_lock = threading.Lock()


def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.page_load_strategy = 'none'

    service = Service('chromedriver-linux64/chromedriver-linux64/chromedriver')

    driver = webdriver.Chrome(options=options, service=service)
    driver.implicitly_wait(10)
    driver.set_page_load_timeout(10)

    return driver


def safe_get(thread_id, driver, wait, link, field):
    tries = 1

    while True:
        if tries % 4 == 0:
            raise Exception('womp womp')

        timeout_handler = TimeoutHandler(10, driver)

        try:
            with timeout_handler:
                driver.get(link)
                sleep(0.5)
                wait.until(EC.visibility_of_element_located(
                    (By.CLASS_NAME, field)))
                break
        except:
            print(f'thread {thread_id}: failed waiting', flush=True)

        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        tries += 1

    return driver, wait


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
        print("Force quitting due to timeout...", flush=True)
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


class ArtistScraper:
    def __init__(self):
        pass

    def scrape(self, thread_id):
        # create driver and waiter
        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        while True:
            response = client.receive_message(
                QueueUrl=os.getenv('AWS_QUEUE_PATH', 'NA'),
                MaxNumberOfMessages=1,
                WaitTimeSeconds=0,
                VisibilityTimeout=900
            )

            if 'Messages' not in response:
                break

            link = response['Messages'][0]['Body']
            link = link.replace('34.201.209.209', '34.224.117.253')
            receipt_handle = response['Messages'][0]['ReceiptHandle']

            artist_names = []
            artist_genres = []
            artist_descriptions = []
            links = []
            artist_id = []

            # request page and wait for body to load
            print(f'thread {thread_id}: processing {link}', flush=True)
            driver, wait = safe_get(thread_id, driver, wait, link, 'table')
            print('done waiting', flush=True)

            # get artist table
            artist_table_elem = driver.find_element(by=By.TAG_NAME,
                                                    value='tbody')

            # get links elements to artists
            artist_link_elems = artist_table_elem.find_elements(by=By.TAG_NAME,
                                                                value='a')

            # replace the concert archive link with root IP
            for link_elem in artist_link_elems:
                link = link_elem.get_attribute('href')
                link = link.replace('www.concertarchives.org', '34.224.117.253')
                links.append(link)

            # for every artist link, scrape artist info
            for link_idx in range(len(links)):
                link = links[link_idx]
                print(f'thread {thread_id}: link progress: {link_idx + 1} / {len(links)}', flush=True)

                # try to load page
                driver, wait = safe_get(thread_id, driver, wait, link, "profile-display")

                # scrape info
                name, genres, description = self.scrape_artist(driver)
                artist_names.append(name)
                artist_genres.append(genres)
                artist_descriptions.append(description)
                artist_id.append(link[link.rfind('/') + 1:])

            genre_strings = list(map(lambda l: ';'.join(l), artist_genres))

            mini_artist_set = pd.DataFrame({
                'artist': artist_names,
                'genres': genre_strings,
                'bio': artist_descriptions,
                'artist_id': artist_id
            })

            client.delete_message(QueueUrl=os.getenv('AWS_QUEUE_PATH', 'NA'),
                                  ReceiptHandle=receipt_handle)

            with sets_lock:
                global master_set
                master_set = pd.concat([master_set, mini_artist_set])
                master_set.to_csv(f'./artist_set_{init_time}.csv', index=False)
                s3.upload_file(f'./artist_set_{init_time}.csv', 'artistbucket777', f'artist_set_{init_time}.csv')

        driver.quit()

    def clean_string(self, s: str):
        for str_idx in range(len(s)):
            if s[str_idx].isalpha():
                first = str_idx
                break

        for str_idx in range(len(s) - 1, -1, -1):
            if s[str_idx].isalpha():
                last = str_idx
                break

        return s[first: last + 1]

    def scrape_artist_name(self, driver):
        name_elem = driver.find_element(by=By.CLASS_NAME,
                                        value='profile-display')
        end = ' Concert History'
        name = name_elem.text[:-len(end)]
        return name

    def scrape_artist_genres(self, driver):
        genres = []

        driver.implicitly_wait(0)
        more_geners = driver.find_elements(by=By.ID, value='show-more-list-genres')

        if len(more_geners) == 1:
            more_geners[0].click()
            sleep(0.1)

        genre_elems = driver.find_elements(by=By.CLASS_NAME, value='genre-list')
        driver.implicitly_wait(20)

        for genre_elem in genre_elems:
            try:
                genres.append(self.clean_string(genre_elem.text))
            except Exception as e:
                pass

        return genres

    def scrape_artist_description(self, driver):
        description_elem = driver.find_element(by=By.CLASS_NAME,
                                               value='header-bio')
        description = description_elem.text
        return description

    def scrape_artist(self, driver):
        name = self.scrape_artist_name(driver)
        genres = self.scrape_artist_genres(driver)
        description = self.scrape_artist_description(driver)

        return name, genres, description


class ConcertScraper:
    def __init__(self):
        pass

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

    def scrape_concert_bands(self, concert_elems, concert_name):
        bands_elem = concert_elems[1].find_elements(by=By.CLASS_NAME,
                                                    value='concert-index-band-list')

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

    def scrape_concerts(self, concert_elems):
        concert_name = self.scrape_concert_name(concert_elems)
        start_date, end_date = self.scrape_concert_date(concert_elems)
        band = self.scrape_concert_bands(concert_elems, concert_name)
        venue = self.scrape_concert_venue(concert_elems)
        city, state, country = self.scrape_concert_location(concert_elems)

        return concert_name, start_date, end_date, band, venue, city, state, country

    def scrape(self):
        start_dates = []
        end_dates = []
        concert_names = []
        bands = []
        venues = []
        cities = []
        states = []
        countries = []

        options1 = webdriver.ChromeOptions()
        options1.add_argument("start-maximized")
        options1.add_argument("--headless")
        options1.add_experimental_option("excludeSwitches",
                                         ["enable-automation"])
        options1.add_experimental_option('useAutomationExtension', False)
        service1 = Service('chromedriver-win64\\chromedriver.exe')
        driver = webdriver.Chrome(options=options1, service=service1)

        for page in range(1, 2):
            # driver.get(f'http://34.201.209.209/locations/new-york-new-york-united-states?page={page}#concert-table')
            concert_list = driver.find_elements(by=By.TAG_NAME, value='tbody')
            concert_list_elems = concert_list[0].find_elements(by=By.TAG_NAME,
                                                               value='tr') + \
                                 concert_list[1].find_elements(by=By.TAG_NAME,
                                                               value='tr')

            for concert_elem in concert_list_elems:
                concert_elems = concert_elem.find_elements(by=By.TAG_NAME,
                                                           value='td')

                concert_name, start_date, end_date, conc_bands, venue, city, state, country = self.scrape_concerts(
                    concert_elems)
                concert_names.append(concert_name)
                start_dates.append(start_date)
                end_dates.append(end_date)
                bands.append(conc_bands)
                venues.append(venue)
                cities.append(city)
                states.append(state)
                countries.append(country)

        concert_set = pd.DataFrame({
            'concert': concert_names,
            'start_date': start_dates,
            'end_date': end_dates,
            'bands': bands,
            'venue': venues,
            'city': cities,
            'state': states,
            'country': countries
        })

        return concert_set


if __name__ == "__main__":
    while True:
        try:
            artist_scraper = ArtistScraper()
            threads = []

            artist_scraper.scrape(1)

            s3.upload_file(f'artist_set_{init_time}.csv', 'artistbucket777', f'artist_set_{init_time}.csv')
        except Exception as e:
            pass

import json
import csv
import requests
import time
import re
import logging   
import traceback
import pandas as pd

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

from bs4 import BeautifulSoup
from datetime import datetime, timedelta

RUN_MODE = 'test'

DRIVER_PATH = 'D:/Web Scraping/chromedriver' # change the driver path
ZM_WEBPAGE = 'https://www.zap-map.com/live/'
CSV_PATH = 'D:/Web Scraping/webscrap/webscrap' #change csv path

ADDRESS_FILE = 'D:/Web Scraping/webscrap/webscrap/ev_addr.csv' # smaller address listing for testing
#ADDRESS_FILE = '/Users/lixinhuang/Downloads/data-1597774123581.csv' # full address listing

PROVIDER = 'zapmap'
CITY = 'Birmingham'


MAX_WAIT = 10
MAX_RETRY = 10
MAX_SCROLLS = 40

START_TIME = str(datetime.now())[0:19]

HEADER = [
    'provider',
    'run_mode',
    'city',
    'start_time',
    'scrap_time',
    'station_name',
    'device_id',
    'device_type',
    'network',
    'kw',
    'status_label',
    'available_connectors',
    'total_connectors',
    'lat',
    'lng',
    'address',
    'data'
]

class ZapMapScraper:

    def __init__(self, debug=False):
        self.debug = debug
        self.driver = self.__get_driver()
        self.logger = self.__get_logger()
        self.wait = WebDriverWait(self.driver, MAX_WAIT)
        

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        self.driver.close()
        self.driver.quit()

        return True
    
    def __scroll(self):
        self.wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="root"]/div/div/div[2]/div[2]')))
        iframe = self.driver.find_element_by_xpath('//*[@id="root"]/div/div/div[2]/div[2]')
        self.driver.switch_to.frame(iframe)
        scrollable_div = self.driver.find_element_by_xpath('//*[@id="root"]/div/div/div[2]/div[2]/div/div[2]/div/div[3]/div')
        self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)

    def __get_logger(self):
        # create logger
        logger = logging.getLogger('zapmaps-scraper')
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        fh = logging.FileHandler('zm-scraper.log')
        fh.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # add formatter to ch
        fh.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(fh)
        return logger


    def __get_driver(self, debug=False):
        options = Options()
        if not self.debug:
            options.add_argument("--headless")
        options.add_argument("--window-size=1366,768")
        options.add_argument("--disable-notifications")
        options.add_experimental_option('prefs', {'intl.accept_languages': 'en_GB'})
        input_driver = webdriver.Chrome(DRIVER_PATH, options=options)
        return input_driver


    # util function to clean special characters
    def __filter_string(self, str):
        strOut = str.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        return strOut
    
    # to close the login popup
    def close_popup(self):
        try:
            close_icon_path = '/html/body/div[3]/div[2]/div/div/div/div[1]/span'
            close_icon_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, close_icon_path)))
            close_icon_btn.click()
        except:
            pass
        try:
            alert = self.driver.switch_to.alert()
            alert.close()
        except:
            pass
        return
    
    def get_station_address(self, url, provider, city):
        self.driver.get(url)
        self.driver.maximize_window()
        time.sleep(2)
        
        print('\n\n### start_time:', START_TIME)
        
        # accept cookies
        try:
            accept_cookies_path = '//*[@id="CybotCookiebotDialogBodyButtonAccept"]'
            accept_cookies_btn = self.wait.until(EC.visibility_of_element_located((By.XPATH, accept_cookies_path)))
            accept_cookies_btn.click()
            time.sleep(10)
        
            # switch to required frame
            self.wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="body"]/div/iframe')))
            iframe = self.driver.find_element_by_xpath('//*[@id="body"]/div/iframe')
            self.driver.switch_to.frame(iframe)
        except TimeoutException as e:
            print ("Failed")
            print(e)
            
        # click on Nearby Chargers
        self.driver.switch_to.frame(0)
        nearby_chargers_path = '//*[@id="root"]/div/div/div[2]/div[1]/div/div[1]/div/ul/li[4]/a'
        nearby_chargers_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, nearby_chargers_path)))
        nearby_chargers_btn.click()

        stations = []
        pattern = '( [A-Z\d]+)?( [A-Z\d]+)?, United Kingdom'
        
        # change the data file path to ur local       
        with open(ADDRESS_FILE, "r") as fh:
            
            lines = csv.reader(fh)
            next(lines) # skip header

            for line in lines:
                try:
                    address = line[1]                    
                    new_addr = re.sub(pattern, '', address)
                    print('\n### search_address:', new_addr)
                    
                    search_box_path = '//*[@id="root"]/div/div/div[3]/div/div/div[1]/div/div[7]/div/input'
                    search_box = self.wait.until(EC.element_to_be_clickable((By.XPATH, search_box_path)))
                    search_box.clear()  # clear the previous input address
                    
                    search_box.send_keys(new_addr)
                    search_box.send_keys(Keys.DOWN)
                    #search_box.send_keys(Keys.RETURN)

                    dropdown_path = '/html/body/div[2]/div[1]'
                    dropdown = self.wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_path)))
                    dropdown.click()
                    time.sleep(2)

                    sugs = self.driver.find_element_by_xpath('//*[@id="root"]/div/div/div[2]/div[2]/div/div[2]/div/div[1]/div')
                    self.driver.execute_script('arguments[0].scrollIntoView()', sugs)
                    time.sleep(2)
                    #scroll_box = self.driver.find_element_by_xpath('//*[@id="root"]/div/div/div[2]/div[2]/div/div[2]/div/div[1]/div')
                    
                    '''
                    last_ht, ht = 0, 1
                    while last_ht != ht:
                        last_ht = ht
                        time.sleep(1)
                        ht = self.driver.execute_script("""
                            arguments[0].scrollTo(0, arguments[0].scrollHeight); 
                            return arguments[0].scrollHeight;
                            """, scroll_box)
                    '''
                    #//div[@class='section-layout section-scrollbox scrollable-y scrollable-show section-layout-flex-vertical']
                    page = BeautifulSoup(self.driver.page_source, 'html.parser')
                    station_list = page.find_all('h2')
                    sz = len(station_list)
                    print('number of stations on this page', sz)

                    for i in range(sz):
                        try:
                            # click on station
                            station_path = '//*[@id="root"]/div/div/div[2]/div[2]/div/div[2]/div/div[1]/div/div['+ str(i+1)+']'
                            station_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, station_path)))
                            station_btn.click()
                            time.sleep(3)

                            print('\n## station:', i)

                            # click on info-box
                            info_box_path = '//*[@id="root"]/div/div/div[3]/div/div/div[1]/div/div[1]/div[3]/div/div[4]/div/div'
                            info_box_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, info_box_path)))
                            info_box_btn.click()
                            time.sleep(2)
                            
                            self.close_popup()
                            
                            try:
                                location = self.driver.find_element_by_xpath('//*[@id="info-box-container"]/div[1]/div/h2').text
                            except:
                                location = 'null'
                            print('== location:', location)
                            
                            try:
                                name_xpath = "//div[@id='info-box-container']/div[1]/div[1]/h2[1]"
                                #station_name = self.driver.find_element_by_xpath('//*[@id="info-box-container"]/div[1]/div/div/div/div[1]/div/span[2]').text
                                station_name = self.driver.find_element_by_xpath(name_xpath).text
                            except:
                                station_name = 'null'                
                            print('== station_name:', station_name)
                            
                            try:
                                dvid_xpath = "//h3[@class='sc-hBbWxd egvyoW']"
                                dvid = self.driver.find_element_by_xpath(dvid_xpath).text
                            except:
                                dvid = 'null'
                            print('== device_id:', dvid)
                            
                           
                            try:
                                type_xpath = "//div[@id='info-box-container-pane-locationDetails']/div[1]/div[1]/div[1]/div[3]/div[7]/div[1]/span[2]"
                                #device_type = self.driver.find_element_by_xpath('//*[@id="info-box-container-pane-locationDetails"]/div/div[1]/div/div[3]/div[5]/div[1]/span[2]').getText()
                                device_type_txt = self.driver.find_element_by_xpath(type_xpath).text

                                pwpt = "(.+) \(([\d\.]+)\s*kW\)"
                                match = re.search(pwpt, device_type_txt)

                                if match:
                                    device_type = match.group(1)
                                    kw = match.group(2)
                                else:
                                    device_type = device_type_txt
                                    kw = 'null'
                            except:
                                device_type = 'null'
                                kw = 'null'
                                
                            print('== device_type:', device_type)
                            print('== kw:', kw)
                            
                            try:
                                network_xpath = "//span[@class='sc-htoDjs fqhcPA']"
                                network = self.driver.find_element_by_xpath(network_xpath).text
                            except:
                                network = 'null'
                            
                            print('== network:', network)

                            
                            try:
                                status = self.driver.find_element_by_xpath('//*[@id="info-box-container-pane-locationDetails"]/div/div[1]/div/div[3]/div[3]/div/div').text
                            except:
                                status = 'null'       
                            print('== status:', status)

                            # click on more-info
                            time.sleep(2)
                            try:
                                more_info_path = '//*[@id="info-box-container-tab-moreInfo"]'
                                more_info_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, more_info_path)))
                                more_info_btn.click()
                                time.sleep(2)

                                station_address = self.driver.find_element_by_xpath('//*[@id="info-box-container-pane-moreInfo"]/div/div[1]/div[2]/span[2]').text
                                #additional_info = self.driver.find_element_by_xpath('//*[@id="info-box-container-pane-moreInfo"]').text
                                #print('== additional_info:', additional_info)
                            except:
                                station_address = 'null'
                            
                            print('== address:', station_address)

                            scrap_time = str(datetime.now())[0:19]

                            item = {
                                'provider': provider,
                                'run_mode': RUN_MODE,
                                'city': city,
                                'start_time': START_TIME,
                                'scrap_time': scrap_time,
                                'station_name': station_name,
                                'device_id': dvid, 
                                'device_type': device_type,
                                'network': network,
                                'kw': kw,
                                'status_label': status,
                                'available_connectors': 'null',
                                'total_connectors': '1',
                                'lat': '',
                                'lng': '',
                                'address': station_address,
                                'data': '' #additional_info
                            }
                            
                            print('== item = ', item)
                            stations.append(item)
                            
                            # return to station list
                            close_info_path = '//*[@id="info-box-container"]/div[1]/span'
                            close_info_icon = self.wait.until(EC.element_to_be_clickable((By.XPATH, close_info_path)))
                            close_info_icon.click()

                        except:
                            continue
                            
                        # sleep before click the next station
                        time.sleep(5)

                except:
                    continue
                    
                # sleep before searching a new address
                time.sleep(10)
                
        return stations


def csv_writer(outfile='zm_ev_stations.csv'): # change path
    targetfile = open(outfile, mode='w', encoding='utf-8', newline='\n')
    writer = csv.writer(targetfile, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(HEADER)
    return writer, targetfile



# run for one city
with ZapMapScraper(debug=True) as scraper:

    stations = scraper.get_station_address(ZM_WEBPAGE, PROVIDER, CITY)
    print('\n\nTotal number of charging stations:', len(stations))
    
    # store data in CSV file
    fmt_tm = datetime.now().strftime("%Y%m%d_%H%M")
    fnm = '%s_%s_%s.csv' % (PROVIDER.lower().replace(' ', '_'), CITY.lower().replace(' ', '_'), fmt_tm)
    csv_fnm = CSV_PATH + '/' + fnm

    writer, targetfile = csv_writer(csv_fnm)
    for st in stations:
        #print(st)
        row_data = list(st.values())
        writer.writerow(row_data)

    targetfile.close()
    print('\n### CSV file closed:', csv_fnm)
    
    time.sleep(10)
    
                
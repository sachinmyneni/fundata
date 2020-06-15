import requests
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
import numpy as np
from random import randint
from time import sleep
import logging
import os
import urllib3
import socket


def get_contents(url: str, filter: str) -> dict:
    """ For a HTML code return a dict of possible values of interest
        based on filter. If none found, return None """
    pass


logging.basicConfig(filename="spotcrime_scrape.log", level=logging.DEBUG,
                    filemode='a', format='%(asctime)s %(message)s')

base_url = 'https://spotcrime.com'


crime_file = './spotcrime.csv'
# spotcrime_df = pd.read_csv(crime_file, header=0)

try:
    spotcrime_df = pd.read_csv(crime_file, header=0)
    print(spotcrime_df.head())
    print(f"Size of the current data: {spotcrime_df.shape}")
except FileNotFoundError:
    logging.info(f"{crime_file} not found. Will create new one")
    pass
except pd.errors.EmptyDataError:
    logging.error(f"{crime_file} had no data. Renaming and will create new one")
    os.rename(crime_file,crime_file+"_bad")
    pass

empty_df = pd.DataFrame()
try:
    response = requests.get(base_url)
except requests.exceptions.SSLError:
    print("Looks like SSL libraries are not installed?")
    print("????????Or a mismatch in hostname??????????")
    exit

if (response.status_code == 200):
    page = response.text
else:
    logging.error(f"{base_url} reported back {response.status_code}")
    raise ValueError

soup = bs(page, "lxml")
state_tag_list = soup.find(id="states-list-menu").find_all('a')
state_dict = {s.text: base_url+s.get('href') for s in state_tag_list}

# Alabama
for this_state in state_dict:
    state_page = requests.get(state_dict[this_state])
    if (state_page.status_code == 200):
        page = state_page.text
    else:
        logging.error(f"{state_dict[this_state]} reported back {state_page.status_code}")
        # raise ValueError
        continue

    state_soup = bs(page, "lxml")
    st_table = state_soup.find(class_='main-content-column')
    # print(st_table)

    dcr_regex = re.compile('Daily Crime Reports')
    dcr_dict = {}  # Daily Crime Report Dict

    for dcr in st_table.find_all(text=dcr_regex):
        dcr_link = dcr.previous['href']
        dcr_place = ''.join(re.split('/', dcr_link)[-2:-1]).replace('+', '_')

        dcr_dict[dcr_place] = base_url+dcr_link  # https://spotcrime.com/vt/burlington/daily

    ''' Iterate through the place page 2 times. Once with basic link
        and once with basic/more link so as to get all the reports.
        Since we are adding this to a dict {date: link}, the repeated
        dates would overwrite the dict with same value for the key.'''
    # Alabama -> Alexander City
    for this_place in dcr_dict:
        logging.info(f"Getting stats for {this_place}")
        place_page = requests.get(dcr_dict[this_place])
        assert this_place.split("_")[0] in place_page.url, f"{this_place} is not in {place_page.url}"
        if (place_page.status_code == 200):
            page = place_page.text
        else:
            logging.error(f"{dcr_dict[this_place]} reported back {place_page.status_code}")
            raise ValueError
        place_soup = bs(page, "lxml")

        # Links for one place which is part of 1 state
        crime_blotter_table = place_soup.find(class_='main-content-column')

        # Some places have no data
        if crime_blotter_table.find('h3').text == 'No data found.':
            logging.error(f"{this_state}->{this_place} had no data")
            break  #  Since this place has no data go back to the next place.
        # print(crime_blotter_table)
        cb_regex = re.compile('Crime Blotter')
        cbr_date_dict = {}
        # Fill the date-link dict for the place
        for cbrd_text in crime_blotter_table.find_all(text=cb_regex):
            cbrd_link = cbrd_text.previous['href']
            cbr_date_dict[cbrd_text.split(' ')[0]] = base_url+cbrd_link
            # cbr_date_dict['06/04/2019']

        xtra_page = dcr_dict[this_place]+"/more"  # https://spotcrime.com/vt/burlington/daily/more
        # Alabama -> Alexander City (again but with more dates in the archives)
        
        logging.info(f"Getting more stats for {this_place}")
        place_page = requests.get(xtra_page)

        if (place_page.status_code == 200):
            page = place_page.text
        else:
            logging.error(f"{xtra_page} reported back {place_page.status_code}")
            # raise ValueError
            pass
        place_soup = bs(page, "lxml")

        # Links for one place which is part of 1 state
        crime_blotter_table = place_soup.find(class_='main-content-column')

        # Some places have no data
        if crime_blotter_table.find('h3').text == 'No data found.':
            dt_array = np.array([None, None, None, None,None, None, this_place,this_state])
            empty_df = pd.DataFrame(dt_array.reshape(1,-1))
            logging.error(f"{this_state}->{this_place} had no data")
            try:
                df_new = df_new.append(empty_df)
            except NameError:
                df_new = empty_df
            break  

        # print(crime_blotter_table)
        cb_regex = re.compile('Crime Blotter')
        # Fill the date-link dict for the place
        for cbrd_text in crime_blotter_table.find_all(text=cb_regex):
            cbrd_link = cbrd_text.previous['href']
            cbr_date_dict[cbrd_text.split(' ')[0]] = base_url+cbrd_link
            # cbr_date_dict['06/04/2019']

        ''' Now that the dict of {date: link} is complete, go to
            each link and grab the data into a dataframe'''
        # Alabama -> Alexander City -> 2020/04/01
        for this_date in cbr_date_dict:
            this_short_date = '/'.join(this_date.split('/')[:2]) + '/' + this_date.split('/')[-1][2:] #  02/03/19 from 02/03/2019
            # if spotcrime_df in locals():
            try:
                if ((spotcrime_df['State'] == this_state) &
                    (spotcrime_df['Place'] == this_place) &
                    (spotcrime_df['Date'].str.contains(this_short_date))).any():
                    logging.error(f"Skipping {this_state}->{this_place}->{this_date}")
                    continue
            except NameError:
                # logging.info("No spotcrime_df defined? Perhaps first pass")
                pass  # no spotcrime_df defined?
            plc_chk = this_place.split("_")[0]
            assert plc_chk in cbr_date_dict[this_date], f"Mismatch between link: {cbr_date_dict[this_date]}\nand current place we are looking at: {this_place}!"

            try:
                date_page = requests.get(cbr_date_dict[this_date])
            except socket.gaierror:
                logging.error(f"Unable to reach: {cbr_date_dict[this_date]}")
                continue
            except urllib3.exceptions.NewConnectionError:
                logging.error(f"Unable to connect to: {cbr_date_dict[this_date]}")
                sleep(30*60)
                date_page = requests.get(cbr_date_dict[this_date])
            except requests.exceptions.ConnectionError:
                logging.error(f"Unable to connect to: {cbr_date_dict[this_date]}")
                date_page = requests.get(cbr_date_dict[this_date])
            except urllib3.exceptions.MaxRetryError:
                logging.error("Max retry attempts reached.. bugging out..")
                continue
            except http.client.RemoteDisconnected:
                logging.fatal(f"Remote server disconnected from {cbr_date_dict[this_date]}")
                exit

            if (date_page.status_code == 200):
                page = date_page.text
            else:
                logging.error(f"{cbr_date_dict[this_date]} reported back {date_page.status_code}")
                # raise ValueError
                pass
            date_soup = bs(page, "lxml")
            mobl_links = []
            try:
                df = pd.read_html(date_soup.find('table').prettify())
                # Alabama -> Alexander City -> 2020/04/01 -> Details Table
                for det in date_soup.find('table').find_all(text='Details'):
                    mobl_links.append(base_url+det.previous['href'])
                df[0]['Link'] = np.array(mobl_links, dtype=str)
                df[0]['Place'] = this_place
                df[0]['State'] = this_state
                print(df[0])
                if empty_df.size == 0:
                    df_new = df[0]
                else:
                    df_new = empty_df.append(df[0])
                sleep(randint(1, 10))
            except AttributeError:
                logging.error(f"{this_state} -> {this_place} -> {this_date} had no data")
                continue

            with open(crime_file, 'a') as sc_f:
                df_new.to_csv(sc_f, header=sc_f.tell() == 0)
                empty_df = df_new[0:0]  # empty the empty_df

        del cbr_date_dict

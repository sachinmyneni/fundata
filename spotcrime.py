import requests
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
import numpy as np
from random import randint
from time import sleep
import logging
import os
import sys
import urllib3
import socket
import random
import argparse
import break_up_csv as buc

parser = argparse.ArgumentParser()
parser.add_argument("-s","--state", help="Name of the state to scrape for")
parser.add_argument("-p","--place", help="Name of the place to scrape for; State should be provided")
args = parser.parse_args()


logging.basicConfig(filename="spotcrime_scrape.log", level=logging.DEBUG,
                    filemode='a', format='%(asctime)s %(message)s')

base_url = 'https://spotcrime.com'



def get_crime_stats(state_page_link: str, this_state: str, this_place: str = None):
    empty_df = pd.DataFrame()
    state_page = requests.get(state_page_link)
    if (state_page.status_code == 200):
        page = state_page.text
    else:
        logging.error(f"{state_page_link} reported back {state_page.status_code}")
        # raise ValueError
        return  #  Go to the next state

    clean_name = buc.clean_state_name(this_state)
    crime_file = 'spotcrime_'+clean_name+'.csv'
    try:
        spotcrime_df = pd.read_csv(crime_file, header=0)
        print(spotcrime_df.head())
        print(f"Size of the current data: {spotcrime_df.shape}")
    except pd.errors.ParserError:
        logging.error(f"{crime_file} needs to be cleaned up.")
        sys.exit(1)
    except FileNotFoundError:
        logging.info(f"{crime_file} not found. Will create new one")
        pass
    except pd.errors.EmptyDataError:
        logging.error(f"{crime_file} had no data. Renaming and will create new one")
        os.rename(crime_file,crime_file+"_bad")
        pass

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
    while True:
        try:
            if this_place:
                try:
                    this_place = this_place.lower()
                    this_place_link = dcr_dict[this_place]
                except KeyError:
                    logging.error(f"The provided place: {this_place} does not exist in the list of places")
                    return
            else:
                [(this_place,this_place_link)] = random.sample(list(dcr_dict.items()),1)
        except UnboundLocalError:
            [(this_place,this_place_link)] = random.sample(list(dcr_dict.items()),1)
        logging.info(f"Getting stats for {this_place}")
        place_page = requests.get(this_place_link)
        assert this_place.split("_")[0] in place_page.url, f"{this_place} is not in {place_page.url}"
        if (place_page.status_code == 200):
            page = place_page.text
        else:
            logging.error(f"{this_place_link} reported back {place_page.status_code}")
            raise ValueError
        place_soup = bs(page, "lxml")

        # Links for one place which is part of 1 state
        crime_blotter_table = place_soup.find(class_='main-content-column')

        # Some places have no data
        if crime_blotter_table.find('h3').text == 'No data found.':
            logging.error(f"{this_state}->{this_place} had no data")
            continue  #  Since this place has no data go back to the next place.
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
            dt_array = np.array([None, None, None,None, None, this_place,this_state])
            empty_df = pd.DataFrame(dt_array.reshape(1,-1))
            logging.error(f"{this_state}->{this_place} had no data")
            # if df_new:
            #     df_new = df_new.append(empty_df)
            # else:
            #     df_new = empty_df
            if len(cbr_date_dict) == 0:
                try:
                    df_new = df_new.append(empty_df)
                except NameError:
                    df_new = empty_df
                continue  

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
                logging.error(f"Unable to connect to: {cbr_date_dict[this_date]} for the first time")
                sleep(30*60)
                continue
            except requests.exceptions.ConnectionError:
                logging.error(f"Unable to connect to: {cbr_date_dict[this_date]}")
                sleep(30*60)
                continue
            except urllib3.exceptions.MaxRetryError:
                logging.error("Max retry attempts reached.. bugging out..")
                continue
            except http.client.RemoteDisconnected:
                logging.fatal(f"Remote server disconnected from {cbr_date_dict[this_date]}")
                sleep(30*60)
                continue

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
                sleep(randint(1, 5))
            except AttributeError:
                logging.error(f"{this_state} -> {this_place} -> {this_date} had no data")
                dt_array = np.array([None, None, this_date,None, None, this_place, this_state])
                empty_df = pd.DataFrame(dt_array.reshape(1,-1))
                try:
                    df_new = df_new.append(empty_df)
                except NameError:
                    df_new = empty_df

            with open(crime_file, 'a') as sc_f:
                df_new.to_csv(sc_f, header=sc_f.tell() == 0, index=False)
                empty_df = df_new[0:0]  # empty the empty_df

        del cbr_date_dict
        del this_place


def main():
    try:
        response = requests.get(base_url)
    except requests.exceptions.SSLError:
        print("Looks like SSL libraries are not installed?")
        print("????????Or a mismatch in hostname??????????")
        sys.exit()

    if (response.status_code == 200):
        page = response.text
    else:
        logging.error(f"{base_url} reported back {response.status_code}")
        raise ValueError

    soup = bs(page, "lxml")
    state_tag_list = soup.find(id="states-list-menu").find_all('a')
    state_dict = {s.text: base_url+s.get('href') for s in state_tag_list}

    # Alabama

    if args.state:
        this_state = args.state
        state_page_link = state_dict[this_state]
        if args.place:
            get_crime_stats(state_page_link,this_state,args.place)
        else:
            get_crime_stats(state_page_link,this_state)
    else:
        while True:
            [(this_state,state_page_link)] = random.sample(list(state_dict.items()),1)
            get_crime_stats(state_page_link,this_state)

if __name__ == "__main__":
    main()
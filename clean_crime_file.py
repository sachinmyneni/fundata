import pandas as pd
import re
import logging

def badrow(address: str, city: str) -> bool:
    if city.split('_')[0].lower() in address.lower():
        return False
    else:
        return True

logging.basicConfig(filename="spotcrime_scrape.log", level=logging.DEBUG,
                    filemode='a', format='%(asctime)s %(message)s')

crime_file = './spotcrime.csv.bad'

try:
    spotcrime_df = pd.read_csv(crime_file, header=0)
    print(spotcrime_df.head())
except FileNotFoundError:
    logging.fatal(f"{crime_file} not found. Will create new one")
except pd.errors.EmptyDataError:
    logging.fatal(f"{crime_file} had no data. Renaming and will create new one")

spotcrime_df.filter(badrow(spotcrime_df['ADDRESS'],spotcrime_df['CITY']))

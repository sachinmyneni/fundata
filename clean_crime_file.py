import pandas as pd
import re
import logging

def badrow(address: str, city: str) -> bool:
    if city.split('_')[0].lower() in address.lower():
        return False
    else:
        return True

# logging.basicConfig(filename="spotcrime_scrape.log", level=logging.DEBUG,
#                     filemode='a', format='%(asctime)s %(message)s')

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

crime_file = './spotcrime.csv.2'

try:
    spotcrime_df = pd.read_csv(crime_file, header=0)
    print(spotcrime_df.head())
except FileNotFoundError:
    logging.fatal(f"{crime_file} not found.")
except pd.errors.EmptyDataError:
    logging.fatal(f"{crime_file} had no data.")

print(f"Shape before filter: {spotcrime_df.shape}")

# spotcrime_df.filter(badrow(spotcrime_df['Address'],spotcrime_df['Place']))
df_new = spotcrime_df[spotcrime_df.apply(lambda x: x['Place'].split('_')[0].lower() in x['Address'].lower(), axis=1)]

print(f"Shape after filter: {df_new.shape}")

df_new.to_csv('sc_cleaned.csv',header=True, index=False)

import pandas as pd
import os
import re
import shutil
import logging

logging.basicConfig(filename="spotcrime_cleanup_csv.log", level=logging.DEBUG,
                    filemode='a', format='%(asctime)s %(message)s')

def fix_file(fnm: str)->None:
    shutil.copy(fnm,fnm+"_backup")
    temp_csv_name = "temp_"+fnm
    this_state = ''.join(re.findall(r'spotcrime_(\w+).csv',fnm))
    logging.debug(f"{this_state}\t\t{temp_csv_name}")
    df = pd.read_csv(fnm)
    logging.debug(f"Current data size: {df.shape}")
    df.loc[df["State"] != this_state].shift(periods=1,axis='columns').to_csv(temp_csv_name,index=False)
    df.drop(df[df["State"] != this_state].index, inplace=True)
    df.to_csv(temp_csv_name, mode='a',index=False, header=False)
    df = pd.read_csv(temp_csv_name)
    try:
        df.drop_duplicates(subset=['Date', 'Place', 'State', 'Crime', 'Unnamed: 0.1.1'], inplace=True)
        logging.debug(f"Final data size after cleanup: {df.shape}")
        df.to_csv(fnm,mode='w',index=False,header=True)
    except KeyError:
        logging.error(f"{this_state} looks clean. Not overwriting existing file")
    


def main():
    for filename in os.listdir("."):
        if (
            filename.startswith("spotcrime_")
            and filename.endswith(".csv")
            and not os.path.isfile(filename + "_backup")
        ):
            fix_file(filename)

if __name__ == "__main__":
    main()
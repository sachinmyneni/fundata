import pandas as pd
import os

'''  Break up a CSV for all states into individual CSV for each states.
     Return a dict of state_name:file_name
'''

def get_state(bigcsv: str) -> iter:
    all_states = pd.read_csv(bigcsv,usecols=['State'],squeeze=True)
    return iter(all_states.astype('category').cat.categories)



def break_csv(bigcsv: str) -> dict:
    full_df = pd.read_csv(bigcsv, compression='infer')
    dfs = [x for _, x in full_df.groupby('State')]
    # statit = get_state(bigcsv)
    name_file_dict = {}
    for item in dfs:
        state_name = item['State'].iloc[0]
        cleaned_state_name = state_name.replace(' ','_')
        print(f"Cleaned State name from {state_name} to {cleaned_state_name}")
        name_file_dict[state_name] = 'spotcrime_'+cleaned_state_name+'.csv'
        print("Write out the CSV here")

    return name_file_dict

if __name__ == '__main__':
    nfd = break_csv('spotcrime.csv.1.gz')



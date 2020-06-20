import pandas as pd
import os

'''  Break up a CSV for all states into individual CSV for each states.
     Return a dict of state_name:file_name
'''

def get_state(bigcsv: str) -> iter:
    all_states = pd.read_csv(bigcsv,usecols=['State'],squeeze=True)
    return iter(all_states.astype('category').cat.categories)

def testy():
    ndic = {'number': [random.randint(1,30) for i in range(20)]}
    ldic = {'letter': [random.choice(['a','b','c','d']) for i in range(20)]}
    fdic = {'deci':[random.random() for i in range(0,20)]}
    ldic.update(fdic)
    ldic.update(ndic)
    df = pd.DataFrame(ldic)
    dfs = [x for _, x in df.groupby('letter')]

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
        item.to_csv(name_file_dict[state_name])

    return name_file_dict

if __name__ == '__main__':
    nfd = break_csv('spotcrime.csv.1.gz')



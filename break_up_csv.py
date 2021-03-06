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

def clean_state_name(state_name: str) -> str:
    return state_name.replace(' ','_')

def break_csv(bigcsv: str, xtract: bool) -> dict:
    full_df = pd.read_csv(bigcsv, compression='infer')
    dfs = [x for _, x in full_df.groupby('State')]
    # statit = get_state(bigcsv)
    name_file_dict = {}
    for item in dfs:
        state_name = item['State'].iloc[0]
        cleaned_state_name = clean_state_name(state_name)
        print(f"Cleaned State name from {state_name} to {cleaned_state_name}")
        name_file_dict[state_name] = 'spotcrime_'+cleaned_state_name+'.csv'
        if xtract:
                with open(name_file_dict[state_name], 'a') as sc_f:
                    item.to_csv(sc_f, header=sc_f.tell() == 0)

    return name_file_dict

if __name__ == '__main__':
    nfd = break_csv('spotcrime.csv', True)



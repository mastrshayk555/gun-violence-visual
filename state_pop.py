import pandas as pd
from pathlib import Path
import csv
from datetime import datetime

from helpers.state_mappings import us_state_to_abbrev


BASE_DIR = Path.cwd()

data_folder = BASE_DIR.joinpath('data')
pre2020 = data_folder.joinpath('nst-est2020-alldata.csv')
post2020 = data_folder.joinpath('NST-EST2022-ALLDATA.csv')


df1 = pd.read_csv(pre2020)
df2 = pd.read_csv(post2020)

keep_cols1 = [
        'NAME',
        'POPESTIMATE2014', 
        'POPESTIMATE2015', 
        'POPESTIMATE2016', 
        'POPESTIMATE2017', 
        'POPESTIMATE2018', 
        'POPESTIMATE2019', 
    ]

keep_cols2 = [
        'NAME',
        'POPESTIMATE2020', 
        'POPESTIMATE2021', 
        'POPESTIMATE2022',
    ]

rows = [
    'United States', 
    'Northeast Region', 
    'New England', 
    'Middle Atlantic', 
    'Midwest Region', 
    'East North Central', 
    'West North Central', 
    'South Region', 
    'South Atlantic', 
    'East South Central', 
    'West South Central', 
    'West Region', 
    'Mountain',
    'Pacific',
    ]

def drop_columns(df: pd.DataFrame, keep_columns):
    all_columns = list(df.columns)

    drop_cols = []
    for col in all_columns:
        if col not in keep_columns:
            drop_cols.append(col)

    df.drop(columns=drop_cols, inplace=True)


def rename_columns(df: pd.DataFrame, columns):
    col_dict={}
    for col in columns:
        year_str = col.strip('POPESTIMATE')
        col_dict[col]=year_str
    df.rename(columns=col_dict, inplace=True)


def drop_rows(df: pd.DataFrame, rows):
    df = df[~df['N'].isin(rows)]
    df.reset_index(drop=True, inplace=True)
    return df
    

def process_state_pop():
    drop_columns(df1, keep_cols1)
    drop_columns(df2, keep_cols2)
    rename_columns(df1, keep_cols1)
    rename_columns(df2, keep_cols2)
    new_df1 = drop_rows(df1, rows)
    new_df2 = drop_rows(df2, rows)

    comb_df = new_df1.set_index('N').join(new_df2.set_index('N'))
    comb_df.reset_index(inplace=True)
    comb_df.rename(columns={'N': 'state'}, inplace=True)
    return comb_df


if __name__ == '__main__':
    process_state_pop()




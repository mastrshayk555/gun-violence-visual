import pandas as pd
from pathlib import Path
import csv
from datetime import datetime
from helpers.state_mappings import us_state_to_abbrev
from state_pop import process_state_pop


BASE_DIR = Path.cwd()

data_folder = BASE_DIR.joinpath('data')
gun_data = data_folder.joinpath('GunViolenceArchiveDataJan2014toJun2022.csv')


def drop_unused(df: pd.DataFrame):
    cols = ['Address', 'Operations']
    df.drop(columns=cols, inplace=True)


def convert_types(df: pd.DataFrame):
    """Converts index float to int and formats date strings to datetime objects"""
    df.index = df.index.astype(int)
    df['Incident Date'] =  pd.to_datetime(df['Incident Date'], format='%Y-%m-%d')


def add_state_abbr(df: pd.DataFrame):
    """Creates a new column that adds states abbreviations"""
    df['state_abbrev'] = ''
    df['state_abbrev'] = df['state'].apply(lambda x: us_state_to_abbrev.get(x))


def reset_date(date: datetime):
    new_date = datetime(date.year, date.month, 1)
    return new_date


def add_adjusted_date(df: pd.DataFrame):
    """Groups incidencts by state for each year"""
    df['year_month'] = ''
    df['year_month'] = df['Incident Date'].apply(lambda x: reset_date(x))


def group_by_date_state(df: pd.DataFrame, pop_df: pd.DataFrame):
    """
        Breaks down data into year groups, sums incidents by month and combines it all
        back together into a new df
    """
    date_list = set(df['year_month'].tolist())
    df_list = []
    for date in date_list:
        spec_date_df: pd.DataFrame = df.loc[df['year_month']==date]
        df_state_group = spec_date_df.groupby(['state_abbrev'])[['n_killed', 'n_injured']].sum()
        df_state_group['date'] = date
        df_state_group['date'] = pd.to_datetime(df_state_group['date']).dt.date
        grouped_df = get_total_pop(df_state_group, pop_df, str(date.year))
        df_list.append(grouped_df)

    concat_df = pd.concat(df_list)
    concat_df.reset_index(inplace=True)

    return concat_df


def total_incidents(df: pd.DataFrame):
    df['total_incidents'] = ''
    df['total_incidents'] = df['n_killed'] + df['n_injured']
    

def get_total_pop(gun_df: pd.DataFrame, pop_df: pd.DataFrame, year):
    """For each state in each year, add total population for given row"""

    pop_year = pop_df[['state_abbrev', str(year)]]
    pop_year = pop_year.rename(columns={str(year): 'population'})
    comb_df = gun_df.merge(pop_year, on='state_abbrev', how='left')
    return comb_df


def add_percent_col(df):
    df['rate'] = 100 * df['total_incidents'] / df['population']


def prep_data() -> pd.DataFrame:
    gun_df = pd.read_csv(gun_data, encoding = "ISO-8859-1", index_col='Incident ID')
    pop_df = process_state_pop()
    
    drop_unused(gun_df)
    convert_types(gun_df)
    add_state_abbr(gun_df)
    add_state_abbr(pop_df)
    add_adjusted_date(gun_df)
    gun_df = group_by_date_state(gun_df, pop_df)
    total_incidents(gun_df)
    add_percent_col(gun_df)
    gun_df = gun_df.sort_values(by='date')

    print(gun_df['date'])
    print(gun_df.loc[gun_df['date']=='2014-03-01'])
    return gun_df


if __name__ == '__main__':
    prep_data()
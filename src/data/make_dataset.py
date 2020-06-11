# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

import os
path = os.getcwd()
path = path[:path.find('us_election2020')]


general_election_polls2020_url = 'https://projects.fivethirtyeight.com/polls-page/president_polls.csv'
president_approval_polls2020_url = 'https://projects.fivethirtyeight.com/polls-page/president_approval_polls.csv'

raw_filepath = path + 'us_election2020\\data\\raw\\'
processed_filepath = path + 'us_election2020\\data\\processed\\'


def general_election_polls2020_download(general_election_polls2020_url=general_election_polls2020_url):
    """ Download data from url - save immutable .csv file
    """
    
    general_election_polls2020 = pd.read_csv(general_election_polls2020_url, 
                                             parse_dates=['start_date', 'end_date', 'created_at'])
    general_election_polls2020.to_csv(raw_filepath+'president_polls2020.csv', index=False)
    
    return general_election_polls2020


def president_approval_polls2020_download(president_approval_polls2020_url=president_approval_polls2020_url):
    """ Download data from url - save immutable .csv file
    """

    president_approval_polls2020 = pd.read_csv(president_approval_polls2020_url, 
                                               parse_dates=['start_date', 'end_date', 'created_at'])

    president_approval_polls2020.to_csv('..\\..\\data\\raw\\president_approval_polls2020.csv')
    
    return president_approval_polls2020


def general_election_polls2020_preprocessig(raw_filepath=raw_filepath, processed_filepath=processed_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    
    # download president_polls.csv dataset and save to raw folder (immutable data)
    polls2020 = general_election_polls2020_download(general_election_polls2020_url)

    # filter 2020 cycle US President: Biden x Trump
    polls2020 = polls2020[polls2020['cycle'] == 2020]
    polls2020 = polls2020[polls2020['office_type'] == 'U.S. President']
    polls2020 = polls2020[polls2020['candidate_name'].isin(['Joseph R. Biden Jr.', 'Donald Trump'])]
    cols = ['question_id', 'poll_id', 'state', 'pollster_id', 'pollster', 'sample_size', 
            'population', 'start_date', 'end_date', 'created_at', 'url', 'candidate_name', 'candidate_party', 'pct']
    polls2020 = polls2020[cols]

    # only select polls with Trump and Biden as candidates
    polls_index = polls2020['question_id'].value_counts() == 2
    polls_index = polls_index[polls_index.values].index
    polls2020 = polls2020[polls2020['question_id'].isin(polls_index)]
    polls2020_descriptive = polls2020.drop(['candidate_name', 'candidate_party', 'pct'], 
                                           axis='columns').set_index(['poll_id', 'question_id'])
    polls2020_values = polls2020[['poll_id', 'question_id', 'candidate_name', 'pct']]
    polls2020_values = pd.pivot_table(polls2020_values, values=['pct'], columns=['candidate_name'], index=['poll_id', 'question_id'])
    polls2020_values.columns = polls2020_values.columns.get_level_values(1)
    polls2020_values.columns.name = None
    polls2020 = polls2020_descriptive.join(polls2020_values).drop_duplicates()

    # edit data and create new features
    polls2020 = polls2020.rename({'Joseph R. Biden Jr.':'Biden', 'Donald Trump':'Trump'}, axis='columns')
    polls2020['state'] = polls2020['state'].fillna('National')
    polls2020['state'] =polls2020['state'].str.replace(r' CD-[0-9]', '')
    polls2020['diff'] = polls2020['Biden'] - polls2020['Trump']
    population_weight = {'a':0.7, 'v':0.8, 'rv':0.9, 'lv':1.}
    polls2020['population_weight'] = polls2020['population'].replace(population_weight)
    polls2020 = polls2020.sort_values('population_weight')
    polls2020['weight'] = polls2020['sample_size']*polls2020['population_weight']
    polls2020 = polls2020.sort_values(['start_date', 'end_date'])
    polls2020 = polls2020.reset_index()
    polls2020 = polls2020.drop_duplicates(subset=['poll_id', 'pollster_id', 'start_date', 'end_date'], keep='last')

    # save processed data to processed folder
    polls2020.to_csv(processed_filepath+'general_election_polls2020.csv', index=False)

    


def president_approval_polls2020_preprocessig(raw_filepath=raw_filepath, processed_filepath=processed_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    # download president_polls.csv dataset and save to raw folder (immutable data)
    president_approval_polls2020 = general_election_polls2020_download(president_approval_polls2020_url)

    # filter 2020 cycle US President: Biden x Trump
    cols = ['question_id', 'poll_id', 'state', 'politician', 'pollster_id','pollster', 'sample_size', 
            'population', 'start_date', 'end_date', 'created_at', 'url', 'yes', 'no']
    president_approval_polls2020 = president_approval_polls2020[cols]
    president_approval_polls2020['state'] = president_approval_polls2020['state'].fillna('National')
    president_approval_polls2020['politician'] = president_approval_polls2020['politician'].replace({'Donald Trump':'Trump'})
    president_approval_polls2020 = president_approval_polls2020.rename({'yes':'approve', 'no':'disapprove'}, axis='columns')

    # edit data and create new features
    population_weight = {'a':0.7, 'v':0.8, 'rv':0.9, 'lv':1.}
    president_approval_polls2020['population_weight'] = president_approval_polls2020['population'].replace(population_weight)
    president_approval_polls2020['weight'] = president_approval_polls2020['sample_size']*president_approval_polls2020['population_weight']

    # edit data and create new features
    president_approval_polls2020 = president_approval_polls2020.drop_duplicates(subset=['poll_id', 'pollster_id', 'start_date', 'end_date'], keep='last')
    president_approval_polls2020['diff'] = president_approval_polls2020['approve'] - president_approval_polls2020['disapprove']
    president_approval_polls2020 = president_approval_polls2020 = president_approval_polls2020.sort_values(['start_date', 'end_date', 'population_weight'])
                     
    # save processed data to processed folder
    president_approval_polls2020.to_csv(processed_filepath+'president_approval_polls2020.csv', index=False)


if __name__ == '__main__':

    general_election_polls2020_download(raw_filepath, processed_filepath)
    president_approval_polls2020_preprocessig(raw_filepath=raw_filepath, processed_filepath=processed_filepath)

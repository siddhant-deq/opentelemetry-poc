import requests
import pandas as pd
import time

def fetch_data(url):
    response = requests.get(url)
    json_data = response.json()
    data = json_data['data']
    return data


def process_data_to_df(data):
    df_orig = pd.DataFrame(data)
    df_orig = df_orig.drop(columns=['time_until_update'])
    df_orig.columns = ['alternativeme_fear_greed_index_1d', 'alternativeme_fear_greed_class_1d', 'event_timestamp']
    df_orig['event_timestamp'] = pd.to_datetime(df_orig['event_timestamp'].astype(int), unit='s')
    return df_orig

def process_df(df, sleep_time=0):
    df['final_str'] = ''
    for index, row in df.iterrows():
        df.iloc[index] = process_row(row, sleep_time=sleep_time)
    return df

def process_row(row, sleep_time=0):
    time.sleep(sleep_time)
    row['final_str'] = row['alternativeme_fear_greed_class_1d'] + row['alternativeme_fear_greed_index_1d']
    return row

def write_to_csv(df, filename):
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    data = fetch_data(url='https://api.alternative.me/fng/?limit=0&format=json')
    df = process_data_to_df(data)
    df = process_df(df)
    write_to_csv(df, filename='fear_n_greed_index.csv')
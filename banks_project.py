from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
final_table_attri = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'


def log_progress(message):

    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now()  
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
               'Name': col[1].get_text(strip=True), 
                'MC_USD_Billion': col[2].contents[0]
              }
            
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):

    exchange_rate = {
    "EUR": 0.93,
    "GBP": 0.8,
    "INR": 82.95
}
    list = df["MC_USD_Billion"].tolist()
    list = [float("".join(x.split(','))) for x in list]
    df["MC_USD_Billion"] = list

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
#print(df)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)
print(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement2, sql_connection)

query_statement3 = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement3, sql_connection)

log_progress('Process Complete')

sql_connection.close()

log_progress('Server Connection closed')

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Function to extract data from the Wikipedia page
def extract(df):
    """
    Extracts data from the Wikipedia page on the largest banks.
    Parses HTML content, iterates through table rows, and extracts bank names and market capitalization.
    """
    URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    page = requests.get(URL)
    
    soup = BeautifulSoup(page.content, "html.parser")

    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    count = 0

    for row in rows:
        if count >= 10:
            break
        col = row.find_all('td')

        if len(col)!=0:
            data_dict = {"Name": col[1].text.strip(),"MC_USD_Billion": col[2].text.strip()}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count=count+1
    return df
    
# Function to transform data by converting market capitalization to different currencies

def transform(df,url):
    """
    Transforms the extracted data by converting market capitalization to different currencies.
    Reads exchange rates from a CSV file and applies conversion to each currency.
    """
    exchange_currency_df = pd.read_csv(url)
    for index, row in exchange_currency_df.iterrows():
        currency, rate = row['Currency'], row['Rate']
        print(currency, rate)
        df[f'MC_{currency}_Billion'] = df['MC_USD_Billion'].apply(lambda x: float(x) * rate)

    return df

# Function to save data to a CSV file
def load_to_csv(df, csv_path):
    """
    Saves the transformed data to a CSV file.
    """
    df.to_csv(csv_path)


# Function to execute a SQL query and print the result
def load_to_db(df, sql_connection, table_name):
    """
    Loads the transformed data to a SQLite database.
    Creates a table in the database if it doesn't exist, or replaces it if it does.
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Function to execute a SQL query and print the result
def run_query(query_statement, sql_connection):
    """
    Executes a SQL query on the SQLite database connection and prints the result.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Function to log progress
def log_progress(message):
    """
    Logs the progress of the ETL process by writing messages with timestamps to a log file.
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    


## Define file paths and table name...
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = '/home/project/Largest_banks_data.csv'
df = pd.DataFrame(columns=["Name","MC_USD_Billion"])
exchange_csv_url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
#table_attribs = ["Name","MC_USD_Billion"]

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(df)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df,exchange_csv_url)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

# Print the contents of the entire table
query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

# Print the average market capitalization of all the banks in Billion USD.
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

# Print only the names of the top 5 banks
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()

import sqlite3
import pandas as pd
from datetime import date, timedelta


def append_to_sql(table_name, nav_lookback_days=0, db_name='database/mutualfund.db'):
    '''
    parameters:

    returns:

    '''

    fetch_date = (date.today() - timedelta(days=nav_lookback_days)).strftime("%d-%b-%Y") # Date from the data needs to be fetched
    amfi_url = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&frmdt=" + fetch_date # HTTP link of the data to be fetched
    
    # -------------------------Data Fetch------------------------------
    try:        
        df = pd.read_csv(amfi_url, sep=";") # Fetched data saved to a dataframe
    except:
        return 0 # If no data is found(Usually on trading holidays)
    if(len(df.index) < 1000):
        return # If number of fund NAV updates < 1000. Usually on equity market holidays as money market and overseas MF may update NAV
    # -----------------------------------------------------------------


    # --------------------Data Cleaning--------------------------------
    df = df[['Scheme Code', 'Scheme Name', 'Net Asset Value', 'Date']].dropna() # We keep only four columns(useful) of data.
    df = df[~df['Scheme Name'].str.contains("IDCW")] # Only Growth funds are kept for the purpose. IDCWs are eliminated
    df = df[~df['Scheme Name'].str.contains("Overnight|OVERNIGHT", regex=True)] # Eliminating Overnight Funds.
    # df = df[~df['Scheme Name'].str.contains("")]
    df = df[~df['Scheme Name'].str.contains("LIQUID|Liquid", regex=True)]#  Liquid Funds
    # df = df[~df['Scheme Name'].str.contains("")]
    df['Scheme Code'] = df['Scheme Code'].astype(int)
    df['Date'] = pd.to_datetime(df['Date'])
    df.rename(columns={"Scheme Code": "scheme_code", "Scheme Name": "scheme_name", "Net Asset Value":"nav", "Date":"date"}, inplace=True)
    # -----------------------------------------------------------------
    

    # ----------------------SQL Manipulation---------------------------
    connection = sqlite3.connect(db_name) # Connection to SQLite DB
    df.to_sql(name=table_name, if_exists='append', con=connection, index=False) # Saving Dataframe to SQL Table
    connection.close()
    # -----------------------------------------------------------------
    return len(df.index), fetch_date


lookback_days = 3
for i in reversed(range(lookback_days)):
    try:
        print(append_to_sql('test', i, 'testdb.db'))
    except:
        print("Data Already Exists")
        break
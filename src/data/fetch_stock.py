import os  
import yfinance as yf  
import pandas as pd 
from datetime import datetime, timedelta  

def fetch_stock_data(ticker, filename):

    if os.path.exists(filename):

        df_existing = pd.read_csv(filename)

        last_date = pd.to_datetime(df_existing["Date"]).max()

        start_date = last_date + timedelta(days=1)
    else:

        start_date = datetime(2015, 1, 1)
    
    # Get today's date and time
    today = datetime.today()

    if start_date.date() >= today.date():
        print(f"No new data to fetch for {ticker}")
        return

    data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=today.strftime("%Y-%m-%d"))

    if data.empty:
        print(f"No new data available for {ticker}")
        return

    data.reset_index(inplace=True)
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        df_final = pd.concat([df_existing, data], ignore_index=True)
        df_final.drop_duplicates(subset="Date", keep="last", inplace=True)
    else:
        df_final = data  

    df_final.sort_values("Date", inplace=True)

    df_final.to_csv(filename, index=False)

    print(f"Data for {ticker} updated and saved to {filename}")


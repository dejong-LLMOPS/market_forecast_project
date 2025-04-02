import os  
import yfinance as yf  
import pandas as pd 
from datetime import datetime, timedelta  

def fetch_stock_data(ticker, filename):
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        # Convert the "Date" column to datetime objects
        df_existing["Date"] = pd.to_datetime(df_existing["Date"])
        last_date = df_existing["Date"].max()
        start_date = last_date + timedelta(days=1)
    else:
        start_date = datetime(2015, 1, 1)
    
    # Get today's date and time
    today = datetime.today()

    if start_date.date() >= today.date():
        print(f"No new data to fetch for {ticker}")
        return

    # Download new data using yfinance
    data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=today.strftime("%Y-%m-%d"))
    if data.empty:
        print(f"No new data available for {ticker}")
        return

    data.reset_index(inplace=True)
    
    # Ensure the "Date" column in new data is also datetime
    data["Date"] = pd.to_datetime(data["Date"])

    if os.path.exists(filename):
        # Read the existing file and ensure "Date" is datetime for consistency
        df_existing = pd.read_csv(filename)
        df_existing["Date"] = pd.to_datetime(df_existing["Date"])
        df_final = pd.concat([df_existing, data], ignore_index=True)
        df_final.drop_duplicates(subset="Date", keep="last", inplace=True)
    else:
        df_final = data  

    # Sort the final DataFrame by "Date"
    df_final.sort_values("Date", inplace=True)
    df_final.to_csv(filename, index=False)

    print(f"Data for {ticker} updated and saved to {filename}")

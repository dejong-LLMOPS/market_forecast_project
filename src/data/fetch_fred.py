import os
import requests
import pandas as pd
import datetime

def fetch_fred_data(series_id, filename):
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("FRED API KEY IS MISSING IN .env")
        return

    # Determine start date for new data
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        # Convert the 'date' column to datetime objects for consistency
        df_existing['date'] = pd.to_datetime(df_existing['date'])
        last_date = df_existing['date'].max()
        start_date = last_date + pd.Timedelta(days=1)
    else:
        start_date = datetime.datetime(2015, 1, 1)

    today_str = datetime.datetime.today().strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,         # The FRED series identifier (e.g., VIXCLS, CPIAUCSL)
        "api_key": api_key,             # Your FRED API key from the environment variable
        "file_type": "json",            # Request the response in JSON format
        "observation_start": start_date_str,  # Start date for data retrieval
        "observation_end": today_str          # End date for data retrieval
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching data for {series_id}: {response.status_code}")
        return

    observations = response.json().get("observations", [])
    if not observations:
        print(f"No new data for {series_id}")
        return

    # Create a DataFrame from the API response observations and keep only required columns
    df = pd.DataFrame(observations)
    df = df[["date", "value"]]

    # If file exists, append new data to existing data
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        df_existing["date"] = pd.to_datetime(df_existing["date"])
        df_final = pd.concat([df_existing, df], ignore_index=True)
        # Remove duplicate dates, keeping the latest records
        df_final.drop_duplicates(subset="date", keep="last", inplace=True)
    else:
        df_final = df

    # Sort the final DataFrame by date for chronological order
    df_final.sort_values("date", inplace=True)

    # Save the updated data to CSV without the index column
    df_final.to_csv(filename, index=False)
    print(f"Data for {series_id} updated and saved to {filename}")

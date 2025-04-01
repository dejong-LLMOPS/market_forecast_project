# Import required libraries
import os                   # For file and OS interactions
import requests             # To make HTTP requests to the CoinCap API
import pandas as pd         # For data manipulation
import datetime             # To work with dates and times
from dotenv import load_dotenv  # To load environment variables from a .env file

# Load environment variables from .env (make sure COINCAP_API_KEY is defined there)
load_dotenv()

def fetch_crypto_data(coin_id, filename):
    """
    Fetch daily historical price data for a cryptocurrency from CoinCap API using an API key.
    
    Parameters:
    - coin_id: The CoinCap asset identifier (e.g., 'bitcoin', 'ethereum').
    - filename: The CSV file path where data will be saved/updated.
    """
    # Retrieve API key from environment variables
    api_key = os.environ.get("COINCAP_API_KEY")
    if not api_key:
        print("COINCAP_API_KEY not set in environment")
        return

    # Check if the CSV file exists to determine the start date for incremental data loading
    if os.path.exists(filename):
        # Read existing data from CSV
        df_existing = pd.read_csv(filename)
        # Convert 'date' column to datetime and get the most recent date
        last_date = pd.to_datetime(df_existing['date']).max()
        # Set new start date as one day after the last date in the file
        start_date = last_date + pd.Timedelta(days=1)
    else:
        # If file doesn't exist, choose a start date (adjust as needed)
        start_date = datetime.datetime(2015, 1, 1)

    # Get the current date and time as the end date
    today = datetime.datetime.today()
    if start_date.date() >= today.date():
        print(f"No new data to fetch for {coin_id}")
        return

    # Convert start and end dates to Unix timestamps in milliseconds (required by CoinCap)
    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(today.timestamp() * 1000)

    # Define the CoinCap API endpoint for historical data with daily interval ("d1")
    url = f"https://api.coincap.io/v2/assets/{coin_id}/history"
    # Set query parameters: interval, start, and end dates in milliseconds
    params = {
        "interval": "d1",
        "start": start_ms,
        "end": end_ms
    }
    # Prepare headers with the API key for authentication
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    # Make the GET request to the CoinCap API
    response = requests.get(url, params=params, headers=headers)
    # Check if the request was successful (HTTP 200)
    if response.status_code != 200:
        print(f"Error fetching data for {coin_id}: {response.status_code}")
        return

    # Parse the JSON response
    data = response.json()
    # Extract the 'data' field containing historical price records
    records = data.get("data", [])
    if not records:
        print(f"No data returned for {coin_id}")
        return

    # Create a DataFrame from the records. Each record typically includes a timestamp and price.
    df = pd.DataFrame(records)
    # Convert the 'time' field (in ms) to a human-readable date and store it in a new 'date' column
    df["date"] = pd.to_datetime(df["time"], unit="ms").dt.date
    # Rename the 'priceUsd' column to 'price' and convert it to numeric data type
    df["price"] = pd.to_numeric(df["priceUsd"], errors='coerce')
    # Keep only relevant columns (date and price)
    df = df[["date", "price"]]
    # Group by date in case there are multiple entries per day (calculating the average price)
    df = df.groupby("date").agg({"price": "mean"}).reset_index()

    # Append to existing data if file exists; otherwise, use new data directly
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        df_final = pd.concat([df_existing, df], ignore_index=True)
        # Remove duplicate dates, keeping the latest records
        df_final.drop_duplicates(subset="date", keep="last", inplace=True)
    else:
        df_final = df

    # Sort the final DataFrame by date for chronological order
    df_final.sort_values("date", inplace=True)
    # Save the updated data to CSV without the index column
    df_final.to_csv(filename, index=False)
    print(f"Data for {coin_id} updated and saved to {filename}")


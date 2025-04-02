import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables (e.g., API keys)
load_dotenv()

# Import your custom data-fetching modules
from fetch_crypto import fetch_crypto_data
from fetch_stock import fetch_stock_data
from fetch_fred import fetch_fred_data

# Define a mapping for stock tickers to sectors
TICKER_SECTOR = {
    'AAPL': 'Tech', 'MSFT': 'Tech', 'GOOGL': 'Tech', 'AMZN': 'Tech', 'NVDA': 'Tech',
    'WMT': 'Retail', 'TGT': 'Retail', 'COST': 'Retail', 'HD': 'Retail', 'LOW': 'Retail',
    'JNJ': 'Medical', 'PFE': 'Medical', 'MRK': 'Medical', 'UNH': 'Medical', 'ABT': 'Medical',
    'VOO': 'ETF'
}

def add_sector_info(filepath, sector):
    """
    Reads the CSV at filepath, adds a 'sector' column, and writes it back.
    """
    try:
        df = pd.read_csv(filepath)
        df["sector"] = sector
        df.to_csv(filepath, index=False)
        print(f"Added sector '{sector}' to {filepath}.")
    except Exception as e:
        print(f"Error adding sector info to {filepath}: {e}")

def update_crypto_data():
    """
    Update crypto data using the CoinCap API.

    Selected cryptocurrencies:
      - Bitcoin (BTC)
      - Ethereum (ETH)
      - Binance Coin (BNB)
      - Cardano (ADA)
      - Solana (SOL)
      
    Data is saved incrementally to data/crypto/.
    """
    # Define the folder to store crypto data (relative to project root)
    crypto_folder = os.path.join("data", "crypto")
    os.makedirs(crypto_folder, exist_ok=True)

    crypto_coins = [
        {"id": "bitcoin",       "ticker": "BTC"},
        {"id": "ethereum",      "ticker": "ETH"},
        {"id": "binance-coin",  "ticker": "BNB"},
        {"id": "cardano",       "ticker": "ADA"},
        {"id": "solana",        "ticker": "SOL"}
    ]
    
    for coin in crypto_coins:
        coin_id = coin["id"]
        ticker = coin["ticker"]
        # Save each coin’s CSV in the data/crypto folder
        filepath = os.path.join(crypto_folder, f"crypto_{ticker}.csv")
        print(f"Updating crypto data for {ticker}...")
        fetch_crypto_data(coin_id, filepath)
        # Label crypto data with "Crypto" for future sector-based evaluation
        add_sector_info(filepath, "Crypto")
        print(f"Crypto data for {ticker} updated.\n")

def update_stock_data():
    """
    Update stock data for multiple groups:
      - Tech: AAPL, MSFT, GOOGL, AMZN, NVDA
      - Retail: WMT, TGT, COST, HD, LOW
      - Medical: JNJ, PFE, MRK, UNH, ABT
      - S&P ETF: VOO

    Data is saved incrementally to data/stocks/.
    Each file is then updated with a sector label from TICKER_SECTOR.
    """
    # Define the folder to store stock data
    stocks_folder = os.path.join("data", "stocks")
    os.makedirs(stocks_folder, exist_ok=True)

    stocks = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
        'WMT', 'TGT', 'COST', 'HD', 'LOW',
        'JNJ', 'PFE', 'MRK', 'UNH', 'ABT',
        'VOO'
    ]
    
    for ticker in stocks:
        filepath = os.path.join(stocks_folder, f"{ticker}.csv")
        print(f"Updating stock data for {ticker}...")
        fetch_stock_data(ticker, filepath)
        # Use our TICKER_SECTOR mapping to add a sector label
        sector = TICKER_SECTOR.get(ticker, "Unknown")
        add_sector_info(filepath, sector)
        print(f"Stock data for {ticker} updated.\n")

def update_fred_data():
    """
    Update macroeconomic data from FRED.
      - VIX (Series ID: VIXCLS)
      - CPI (Series ID: CPIAUCSL)

    Data is saved incrementally to data/fred/.
    """
    # Define the folder to store FRED data
    fred_folder = os.path.join("data", "fred")
    os.makedirs(fred_folder, exist_ok=True)

    fred_series = [
        {"series_id": "VIXCLS",    "filename": os.path.join(fred_folder, "fred_vix.csv")},
        {"series_id": "CPIAUCSL",  "filename": os.path.join(fred_folder, "fred_cpi.csv")}
    ]
    
    for series in fred_series:
        print(f"Updating FRED data for {series['series_id']}...")
        fetch_fred_data(series["series_id"], series["filename"])
        print(f"FRED data for {series['series_id']} updated.\n")

if __name__ == "__main__":
    print("Starting complete data update...")
    update_crypto_data()
    update_stock_data()
    update_fred_data()
    print("All data update processes are complete.")

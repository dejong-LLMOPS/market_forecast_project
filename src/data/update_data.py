import os
import fetch_crypto_data from fetch_crypto
import fetch_stock from fetch_stock
import fetch_fred from fetch_fred
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


TICKER_SECTOR = {
    'AAPL': 'Tech', 'MSFT': 'Tech', 'GOOGL': 'Tech', 'AMZN': 'Tech', 'NVDA': 'Tech',
    'WMT': 'Retail', 'TGT': 'Retail', 'COST': 'Retail', 'HD': 'Retail', 'LOW': 'Retail',
    'JNJ': 'Medical', 'PFE': 'Medical', 'MRK': 'Medical', 'UNH': 'Medical', 'ABT': 'Medical',
    'VOO': 'ETF'
}


def add_sector_info(filepath,sector)
import os
import json
import logging
from datetime import datetime
import time

import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('commodity_tracker.log'),
        logging.StreamHandler()
    ]
)

class CommodityPriceTracker:
    def __init__(self):
        try:
            # Load credentials
            creds_json = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
            
            if not creds_json:
                try:
                    with open('service_account.json', 'r') as f:
                        creds_json = f.read()
                    logging.info("Successfully loaded credentials from service_account.json")
                except FileNotFoundError:
                    raise ValueError("No credentials found in environment or service_account.json")
            
            # Parse JSON credentials
            creds_dict = json.loads(creds_json)
            
            self.scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            
            self.creds = ServiceAccountCredentials.from_json_keyfile_dict(
                creds_dict, 
                self.scope
            )
            
            self.client = gspread.authorize(self.creds)
            logging.info("Successfully authenticated with Google Sheets")
            
        except Exception as e:
            logging.error(f"Authentication Error: {e}")
            raise

        # Market symbols configuration
        self.symbols = {
            'FX': {
                'DX-Y.NYB': 'DXY',
                'EURUSD=X': 'EURUSD',
                'NZDUSD=X': 'NZDUSD',
                'USDKRW=X': 'USDKRW',
                'USDTHB=X': 'USDTHB',
                'USDSGD=X': 'USDSGD'
            },
            'Energy': {
                'BZ=F': 'Brent',
                'CL=F': 'WTI'
            },
            'Feed': {
                'ZC=F': 'Corn',
                'ZM=F': 'Soybean'
            },
            'Metals': {
                'GC=F': 'Gold',
                'SI=F': 'Silver'
            },
            'Crypto': {
                'BTC-USD': 'Bitcoin'
            }
        }

    def fetch_commodity_prices(self):
        """Fetch current prices."""
        results = {}
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        for category, symbols_dict in self.symbols.items():
            category_data = {'Date': current_date}
            
            for symbol, display_name in symbols_dict.items():
                try:
                    time.sleep(1)  # Prevent rate limiting
                    
                    stock = yf.Ticker(symbol)
                    history = stock.history(period="1d")
                    
                    if not history.empty:
                        current_price = history['Close'].iloc[-1]
                        category_data[display_name] = round(current_price, 4)
                        logging.info(f"Successfully fetched data for {display_name}")
                    else:
                        logging.warning(f"No data retrieved for {display_name}")
                        category_data[display_name] = 'N/A'
                
                except Exception as e:
                    logging.error(f"Error fetching data for {display_name}: {e}")
                    category_data[display_name] = 'N/A'
            
            results[category] = [category_data]
        
        return results

    def update_google_sheet(self, data):
        """Update Google Sheet with data."""
        try:
            sheet = self.client.open('Commodity Price Tracker')
            
            for category, prices in data.items():
                try:
                    time.sleep(2)  # Delay between worksheet updates
                    
                    try:
                        worksheet = sheet.worksheet(category)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sheet.add_worksheet(category, 1000, 20)
                        logging.info(f"Created new worksheet for {category}")
                    
                    # Get existing data for WoW calculations
                    existing_data = worksheet.get_all_values()
                    time.sleep(2)
                    
                    last_week_prices = {}
                    if len(existing_data) > 1:
                        last_row = existing_data[-1]
                        headers = existing_data[0]
                        
                        for idx, header in enumerate(headers):
                            if header not in ['Date']:
                                if not header.endswith('WoW'):
                                    last_week_prices[header] = float(last_row[idx]) if last_row[idx] != 'N/A' else None

                    if prices and prices[0]:
                        new_headers = ['Date']
                        new_row = [prices[0]['Date']]
                        
                        # Create headers and prepare data
                        for key, value in prices[0].items():
                            if key != 'Date':
                                new_headers.extend([key, f"{key} WoW"])
                                new_row.append(value)
                                
                                # Calculate WoW change
                                if key in last_week_prices and last_week_prices[key] is not None and value != 'N/A':
                                    try:
                                        wow_change = (float(value) - float(last_week_prices[key])) / float(last_week_prices[key])
                                        new_row.append(wow_change)
                                    except (ValueError, TypeError):
                                        new_row.append('N/A')
                                else:
                                    new_row.append('N/A')
                        
                        # Update worksheet
                        worksheet.clear()
                        time.sleep(2)
                        
                        worksheet.append_row(new_headers)
                        time.sleep(2)
                        
                        worksheet.append_row(new_row)
                        time.sleep(2)
                        
                        # Format the sheet
                        num_columns = len(new_headers)
                        
                        # Format headers
                        header_format = {
                            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER"
                        }
                        worksheet.format(f'A1:{chr(64 + num_columns)}1', header_format)
                        time.sleep(2)
                        
                        # Format WoW columns as percentages
                        for col in range(1, num_columns + 1):
                            header = new_headers[col - 1]
                            if header.endswith('WoW'):
                                col_letter = chr(64 + col)
                                worksheet.format(f'{col_letter}2', {
                                    "numberFormat": {"type": "PERCENT", "pattern": "0.00%"}
                                })
                        
                        logging.info(f"Successfully updated {category} worksheet")
                    else:
                        logging.warning(f"No data to write for {category}")
                
                except Exception as category_error:
                    logging.error(f"Error updating {category} worksheet: {category_error}")
                    continue
            
            logging.info("Successfully updated all Google Sheets")
        
        except Exception as e:
            logging.error(f"Critical error updating Google Sheets: {e}")
            raise

    def run(self):
        """Main execution method."""
        try:
            logging.info("Starting commodity price tracking...")
            prices = self.fetch_commodity_prices()
            
            if not prices:
                raise ValueError("No price data was fetched")
            
            self.update_google_sheet(prices)
            logging.info("Commodity price tracking completed successfully")
            
        except Exception as e:
            logging.critical(f"Unexpected error in run method: {e}")
            raise

def main():
    try:
        print("Starting price tracker...")
        logging.info("Initializing Commodity Price Tracker")
        tracker = CommodityPriceTracker()
        tracker.run()
        print("Price tracking completed!")
    except Exception as e:
        print(f"Error occurred: {e}")
        logging.critical(f"Failed to initialize or run tracker: {e}")
        raise

if __name__ == "__main__":
    main()
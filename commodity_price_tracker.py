
# Previous imports remain the same
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
        # Previous initialization code remains the same
        try:
            creds_json = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
            
            if not creds_json:
                try:
                    with open('service_account.json', 'r') as f:
                        creds_json = f.read()
                    logging.info("Successfully loaded credentials from service_account.json")
                except FileNotFoundError:
                    raise ValueError("No credentials found in environment or service_account.json")
            
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

        # Market symbols configuration remains the same
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

    def sleep_with_backoff(self, base_delay=1):
        """Sleep with exponential backoff to respect API limits."""
        time.sleep(base_delay)

    def generate_market_commentary(self, category, price_data, prev_data=None):
        """Generate market commentary with trend analysis if previous data available."""
        try:
            if category == 'FX':
                dxy = price_data.get('DXY', 'N/A')
                if dxy != 'N/A':
                    trend = ""
                    if prev_data and 'DXY' in prev_data:
                        prev_dxy = float(prev_data['DXY'])
                        curr_dxy = float(dxy)
                        if curr_dxy > prev_dxy:
                            trend = " Strengthening trend."
                        elif curr_dxy < prev_dxy:
                            trend = " Weakening trend."
                    
                    if float(dxy) > 103:
                        return f"USD showing strength across major currencies. Asian currencies under pressure.{trend}"
                    elif float(dxy) < 100:
                        return f"USD weakness prevalent. Favorable for emerging Asian currencies.{trend}"
                    else:
                        return f"USD trading in neutral range. Mixed performance across currency pairs.{trend}"
            
            # Similar modifications for other categories...
            
        except Exception as e:
            logging.error(f"Error generating commentary for {category}: {e}")
            return "Market commentary unavailable"

    def fetch_commodity_prices(self):
        """Fetch current prices with previous data for trend analysis."""
        results = {}
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        for category, symbols_dict in self.symbols.items():
            category_data = {'Date': current_date}
            
            for symbol, display_name in symbols_dict.items():
                try:
                    time.sleep(1)
                    
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
            
            # Get previous data for trend analysis
            try:
                worksheet = self.client.open('Commodity Price Tracker').worksheet(category)
                existing_data = worksheet.get_all_records()
                prev_data = existing_data[-1] if existing_data else None
            except Exception:
                prev_data = None
            
            category_data['Market Commentary'] = self.generate_market_commentary(category, category_data, prev_data)
            results[category] = [category_data]
        
        return results

    def format_new_row(self, worksheet, row_number, num_columns):
        """Format a newly added row."""
        try:
            # Format data cells
            data_format = {
                "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                "horizontalAlignment": "CENTER",
                "borders": {
                    "top": {"style": "SOLID"},
                    "bottom": {"style": "SOLID"},
                    "left": {"style": "SOLID"},
                    "right": {"style": "SOLID"}
                }
            }
            
            # Apply basic formatting
            worksheet.format(f'A{row_number}:{chr(64 + num_columns)}{row_number}', data_format)
            time.sleep(1)
            
            # Auto-resize columns if needed
            try:
                worksheet.columns_auto_resize(0, num_columns)
            except Exception as e:
                logging.warning(f"Could not auto-resize columns: {e}")
            
        except Exception as e:
            logging.error(f"Error formatting row: {e}")

    def update_google_sheet(self, data):
        """Update Google Sheet by appending new data rows."""
        try:
            sheet = self.client.open('Commodity Price Tracker')
            
            for category, prices in data.items():
                try:
                    time.sleep(2)
                    
                    try:
                        worksheet = sheet.worksheet(category)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sheet.add_worksheet(category, 1000, 20)
                        logging.info(f"Created new worksheet for {category}")
                    
                    # Get existing data
                    existing_data = worksheet.get_all_values()
                    time.sleep(2)
                    
                    if not existing_data:
                        # Initialize sheet with headers
                        headers = ['Date']
                        for key in prices[0].keys():
                            if key != 'Date':
                                if key != 'Market Commentary':
                                    headers.extend([key, f"{key} WoW"])
                                else:
                                    headers.append(key)
                        
                        worksheet.append_row(headers)
                        time.sleep(2)
                        
                        # Format headers
                        header_format = {
                            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                            "borders": {
                                "top": {"style": "SOLID"},
                                "bottom": {"style": "SOLID"},
                                "left": {"style": "SOLID"},
                                "right": {"style": "SOLID"}
                            }
                        }
                        worksheet.format(f'A1:{chr(64 + len(headers))}1', header_format)
                        existing_data = [headers]
                    
                    headers = existing_data[0]
                    prev_row = existing_data[-1] if len(existing_data) > 1 else None
                    
                    # Prepare new row
                    new_row = [prices[0]['Date']]
                    
                    # Add data and calculate WoW changes
                    for i in range(1, len(headers)):
                        header = headers[i]
                        if header.endswith('WoW'):
                            base_header = header[:-4].strip()
                            if prev_row and len(prev_row) > i-1:
                                try:
                                    current_value = float(prices[0][base_header])
                                    prev_value = float(prev_row[i-1])
                                    wow_change = (current_value - prev_value) / prev_value
                                    new_row.append(wow_change)
                                except (ValueError, KeyError, ZeroDivisionError):
                                    new_row.append('N/A')
                            else:
                                new_row.append('N/A')
                        else:
                            new_row.append(prices[0].get(header, 'N/A'))
                    
                    # Append new row
                    worksheet.append_row(new_row)
                    time.sleep(2)
                    
                    # Format new row
                    row_number = len(existing_data) + 1
                    self.format_new_row(worksheet, row_number, len(headers))
                    
                    # Format WoW columns as percentages
                    for i, header in enumerate(headers):
                        if header.endswith('WoW'):
                            col_letter = chr(65 + i)
                            worksheet.format(f'{col_letter}{row_number}', {
                                "numberFormat": {
                                    "type": "PERCENT",
                                    "pattern": "0.00%"
                                }
                            })
                            time.sleep(1)
                    
                    logging.info(f"Successfully updated {category} worksheet")
                
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

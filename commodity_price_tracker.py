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

    def sleep_with_backoff(self, base_delay=1):
        """Sleep with exponential backoff to respect API limits."""
        time.sleep(base_delay)

    def fetch_commodity_prices(self):
        """Fetch current prices with improved error handling."""
        results = {}
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        for category, symbols_dict in self.symbols.items():
            category_data = {'Date': current_date}
            
            for symbol, display_name in symbols_dict.items():
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                        time.sleep(1)  # Prevent rate limiting
                        
                        stock = yf.Ticker(symbol)
                        history = stock.history(period="1d")
                        
                        if not history.empty:
                            current_price = history['Close'].iloc[-1]
                            category_data[display_name] = round(current_price, 4)
                            logging.info(f"Successfully fetched data for {display_name}")
                            break
                        else:
                            retry_count += 1
                            if retry_count == max_retries:
                                logging.warning(f"No data retrieved for {display_name} after {max_retries} attempts")
                                category_data[display_name] = 'N/A'
                    
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            logging.error(f"Error fetching data for {display_name} after {max_retries} attempts: {e}")
                            category_data[display_name] = 'N/A'
                        time.sleep(2 ** retry_count)  # Exponential backoff
            
            # Add market analysis
            category_data['Market Analysis'] = self.analyze_category(category, category_data)
            results[category] = [category_data]
        
        return results

    def format_worksheet(self, worksheet, num_columns):
        """Apply formatting with batch requests to reduce API calls."""
        try:
            # Get current sheet values
            all_values = worksheet.get_all_values()
            num_rows = len(all_values)
            
            if num_rows < 1:
                return
            
            # Prepare batch formatting request
            batch_requests = {
                "requests": [
                    # Header formatting
                    {
                        "repeatCell": {
                            "range": {
                                "startRowIndex": 0,
                                "endRowIndex": 1,
                                "startColumnIndex": 0,
                                "endColumnIndex": num_columns
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                                    "textFormat": {"bold": True, "fontSize": 11},
                                    "horizontalAlignment": "CENTER",
                                    "borders": {
                                        "top": {"style": "SOLID"},
                                        "bottom": {"style": "SOLID"},
                                        "left": {"style": "SOLID"},
                                        "right": {"style": "SOLID"}
                                    }
                                }
                            },
                            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,borders)"
                        }
                    },
                    # Data cells formatting
                    {
                        "repeatCell": {
                            "range": {
                                "startRowIndex": 1,
                                "endRowIndex": num_rows,
                                "startColumnIndex": 0,
                                "endColumnIndex": num_columns
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                                    "textFormat": {"bold": False, "fontSize": 10},
                                    "horizontalAlignment": "CENTER",
                                    "borders": {
                                        "top": {"style": "SOLID"},
                                        "bottom": {"style": "SOLID"},
                                        "left": {"style": "SOLID"},
                                        "right": {"style": "SOLID"}
                                    }
                                }
                            },
                            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,borders)"
                        }
                    }
                ]
            }
            
            # Apply percentage formatting to WoW columns
            for col in range(1, num_columns + 1):
                header = all_values[0][col - 1]
                if header.endswith('WoW'):
                    batch_requests["requests"].append({
                        "repeatCell": {
                            "range": {
                                "startRowIndex": 1,
                                "endRowIndex": num_rows,
                                "startColumnIndex": col - 1,
                                "endColumnIndex": col
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "numberFormat": {
                                        "type": "PERCENT",
                                        "pattern": "0.00%"
                                    }
                                }
                            },
                            "fields": "userEnteredFormat.numberFormat"
                        }
                    })

            # Execute batch update
            worksheet.spreadsheet.batch_update(batch_requests)
            
            # Auto-resize columns (done separately as it can't be batched)
            for i in range(1, num_columns + 1):
                try:
                    worksheet.columns_auto_resize(i-1, i)
                    self.sleep_with_backoff(0.5)  # Small delay between resizes
                except Exception as e:
                    logging.warning(f"Error auto-resizing column {i}: {e}")
            
            logging.info("Successfully applied formatting to worksheet")
        
        except Exception as e:
            logging.error(f"Error applying worksheet formatting: {e}")
            raise

    def update_google_sheet(self, data):
        """Update Google Sheet with rate limiting and error handling."""
        try:
            sheet = self.client.open('Commodity Price Tracker')
            
            for category, prices in data.items():
                try:
                    self.sleep_with_backoff(2)  # Delay between category updates
                    
                    try:
                        worksheet = sheet.worksheet(category)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sheet.add_worksheet(category, 1000, 20)
                        logging.info(f"Created new worksheet for {category}")
                        self.sleep_with_backoff(1)
                    
                    # Get existing data for WoW calculations
                    existing_data = worksheet.get_all_values()
                    self.sleep_with_backoff(1)
                    
                    last_week_prices = {}
                    if len(existing_data) > 1:
                        last_row = existing_data[-1]
                        headers = existing_data[0]
                        
                        for idx, header in enumerate(headers):
                            if header not in ['Date', 'Market Analysis']:
                                if not header.endswith('WoW'):
                                    last_week_prices[header] = float(last_row[idx]) if last_row[idx] != 'N/A' else None

                    if prices and prices[0]:
                        new_headers = []
                        new_row = []
                        
                        # Create headers and prepare data
                        base_headers = list(prices[0].keys())
                        for header in base_headers:
                            if header not in ['Date', 'Market Analysis']:
                                new_headers.extend([header, f"{header} WoW"])
                                
                                current_price = prices[0][header]
                                new_row.append(current_price)
                                
                                # Calculate WoW change
                                if header in last_week_prices and last_week_prices[header] is not None:
                                    try:
                                        wow_change = (float(current_price) - float(last_week_prices[header])) / float(last_week_prices[header])
                                        new_row.append(wow_change)
                                    except (ValueError, TypeError):
                                        new_row.append('N/A')
                                else:
                                    new_row.append('N/A')
                            else:
                                new_headers.append(header)
                                new_row.append(prices[0][header])
                        
                        # Update worksheet
                        worksheet.clear()
                        self.sleep_with_backoff(1)
                        
                        worksheet.insert_row(new_headers, 1)
                        self.sleep_with_backoff(1)
                        
                        worksheet.append_row(new_row)
                        self.sleep_with_backoff(1)
                        
                        # Apply formatting
                        self.format_worksheet(worksheet, len(new_headers))
                        self.sleep_with_backoff(2)
                        
                        logging.info(f"Successfully updated {category} worksheet")
                    else:
                        logging.warning(f"No data to write for {category}")
                
                except Exception as category_error:
                    logging.error(f"Error updating {category} worksheet: {category_error}")
                    self.sleep_with_backoff(5)  # Longer delay after error
                    continue
            
            logging.info("Successfully updated all Google Sheets")
        
        except Exception as e:
            logging.error(f"Critical error updating Google Sheets: {e}")
            raise

    def run(self):
        """Main execution method with retry logic."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logging.info("Starting commodity price tracking...")
                prices = self.fetch_commodity_prices()
                
                if not prices:
                    raise ValueError("No price data was fetched")
                
                self.update_google_sheet(prices)
                logging.info("Commodity price tracking completed successfully")
                break
                
            except Exception as e:
                retry_count += 1
                logging.error(f"Attempt {retry_count} failed: {e}")
                if retry_count < max_retries:
                    self.sleep_with_backoff(5 * retry_count)  # Increasing delay between retries
                else:
                    logging.critical("All retry attempts failed")
                    raise

def main():
    try:
        logging.info("Initializing Commodity Price Tracker")
        tracker = CommodityPriceTracker()
        tracker.run()
    except Exception as e:
        logging.critical(f"Failed to initialize or run tracker: {e}")
        raise

if __name__ == "__main__":
    main()
```
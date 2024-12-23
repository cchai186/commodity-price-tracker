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
        """Fetch current prices and restructure data for spreadsheet."""
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
            
            # Add market analysis
            category_data['Market Analysis'] = self.analyze_category(category, category_data)
            results[category] = [category_data]
        
        return results

    def analyze_category(self, category, prices_dict):
        """Generate analysis for each category based on current data."""
        try:
            if category == 'FX':
                dxy = prices_dict.get('DXY', 'N/A')
                eur = prices_dict.get('EURUSD', 'N/A')
                
                if dxy != 'N/A' and eur != 'N/A':
                    if float(dxy) > 103:
                        return "USD showing strength across major currencies. Asian currencies under pressure."
                    elif float(dxy) < 100:
                        return "USD weakness prevalent. Favorable for emerging Asian currencies."
                    else:
                        return "USD trading in neutral range. Mixed performance across currency pairs."
            
            elif category == 'Energy':
                brent = prices_dict.get('Brent', 'N/A')
                wti = prices_dict.get('WTI', 'N/A')
                
                if brent != 'N/A' and wti != 'N/A':
                    spread = float(brent) - float(wti)
                    if spread > 5:
                        return f"Wide Brent-WTI spread (${spread:.2f}). Global supply concerns dominate."
                    else:
                        return f"Normal Brent-WTI spread (${spread:.2f}). Market in equilibrium."

            elif category == 'Feed':
                return "Feed costs trending within seasonal ranges. Monitor weather impacts."

            elif category == 'Metals':
                gold = prices_dict.get('Gold', 'N/A')
                
                if gold != 'N/A':
                    if float(gold) > 2000:
                        return "Gold at premium levels. Safe-haven demand strong."
                    else:
                        return "Gold trading below key $2000 level. Monitor Fed policy."

            elif category == 'Crypto':
                btc = prices_dict.get('Bitcoin', 'N/A')
                
                if btc != 'N/A':
                    if float(btc) > 40000:
                        return "BTC maintaining strength above 40K. Institutional interest remains."
                    else:
                        return "BTC below 40K threshold. Market sentiment cautious."

            return "Insufficient data for detailed analysis"

        except Exception as e:
            logging.error(f"Error in analysis for {category}: {e}")
            return "Analysis unavailable"

    def format_worksheet(self, worksheet, num_columns):
        """Apply formatting to worksheet."""
        try:
            # Format headers
            header_format = {
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                "textFormat": {
                    "bold": True,
                    "fontSize": 11
                },
                "horizontalAlignment": "CENTER"
            }
            
            # Format the header row
            worksheet.format(f'A1:{chr(64 + num_columns)}1', header_format)
            
            # Get the number of rows in the worksheet
            all_values = worksheet.get_all_values()
            num_rows = len(all_values)
            
            if num_rows > 1:
                # Format data cells
                data_format = {
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                    "textFormat": {
                        "bold": False,
                        "fontSize": 10
                    },
                    "horizontalAlignment": "CENTER"
                }
                
                # Format percentage columns
                percentage_format = {
                    "numberFormat": {
                        "type": "PERCENT",
                        "pattern": "0.00%"
                    }
                }
                
                # Apply data formatting to all cells
                worksheet.format(f'A2:{chr(64 + num_columns)}{num_rows}', data_format)
                
                # Apply percentage formatting to WoW columns
                for col in range(1, num_columns + 1):
                    header = all_values[0][col - 1]
                    if header.endswith('WoW'):
                        col_letter = chr(64 + col)
                        worksheet.format(f'{col_letter}2:{col_letter}{num_rows}', percentage_format)
                
                # Add borders
                border_format = {
                    "borders": {
                        "top": {"style": "SOLID"},
                        "bottom": {"style": "SOLID"},
                        "left": {"style": "SOLID"},
                        "right": {"style": "SOLID"}
                    }
                }
                
                # Apply borders to entire table
                worksheet.format(f'A1:{chr(64 + num_columns)}{num_rows}', border_format)
                
                # Auto-resize columns
                for i in range(1, num_columns + 1):
                    worksheet.columns_auto_resize(i-1, i)
            
            logging.info("Successfully applied formatting to worksheet")
        
        except Exception as e:
            logging.error(f"Error applying worksheet formatting: {e}")
            raise

    def update_google_sheet(self, data):
        """Update Google Sheet with formatted data and WoW calculations."""
        try:
            sheet = self.client.open('Commodity Price Tracker')
            
            for category, prices in data.items():
                try:
                    try:
                        worksheet = sheet.worksheet(category)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sheet.add_worksheet(category, 1000, 20)
                        logging.info(f"Created new worksheet for {category}")
                    
                    # Get existing data for WoW calculations
                    existing_data = worksheet.get_all_values()
                    last_week_prices = {}
                    if len(existing_data) > 1:  # If there's data beyond headers
                        last_row = existing_data[-1]
                        headers = existing_data[0]
                        
                        # Create dictionary of last week's prices
                        for idx, header in enumerate(headers):
                            if header not in ['Date', 'Market Analysis']:
                                if not header.endswith('WoW'):  # Only get price columns
                                    last_week_prices[header] = float(last_row[idx]) if last_row[idx] != 'N/A' else None

                    # Prepare new data with WoW calculations
                    if prices and prices[0]:
                        new_headers = []
                        new_data = []
                        
                        # Create new headers with WoW columns
                        base_headers = list(prices[0].keys())
                        for header in base_headers:
                            if header not in ['Date', 'Market Analysis']:
                                new_headers.extend([header, f"{header} WoW"])
                            else:
                                new_headers.append(header)
                        
                        # Calculate WoW changes and prepare new data
                        for row in prices:
                            new_row = []
                            for header in base_headers:
                                if header not in ['Date', 'Market Analysis']:
                                    current_price = row[header]
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
                                    new_row.append(row[header])
                        
                        # Clear and update worksheet
                        worksheet.clear()
                        
                        # Add headers and data
                        worksheet.insert_row(new_headers, 1)
                        worksheet.append_row(new_row)
                        
                        # Apply formatting
                        self.format_worksheet(worksheet, len(new_headers))
                        
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
    """Main entry point of the script."""
    try:
        logging.info("Initializing Commodity Price Tracker")
        tracker = CommodityPriceTracker()
        tracker.run()
    except Exception as e:
        logging.critical(f"Failed to initialize or run tracker: {e}")
        raise

if __name__ == "__main__":
    main()
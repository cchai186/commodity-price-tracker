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

    def analyze_category(self, category, prices_dict):
        """Generate analysis for each category based on current and historical data."""
        try:
            if category == 'FX':
                dxy = prices_dict.get('DXY', 'N/A')
                eur = prices_dict.get('EURUSD', 'N/A')
                
                if dxy != 'N/A' and eur != 'N/A':
                    if dxy > 103:
                        return "USD showing strength across major currencies. Asian currencies under pressure."
                    elif dxy < 100:
                        return "USD weakness prevalent. Favorable for emerging Asian currencies."
                    else:
                        return "USD trading in neutral range. Mixed performance across currency pairs."
                
            elif category == 'Energy':
                brent = prices_dict.get('Brent', 'N/A')
                wti = prices_dict.get('WTI', 'N/A')
                
                if brent != 'N/A' and wti != 'N/A':
                    spread = brent - wti
                    if spread > 5:
                        return f"Wide Brent-WTI spread (${spread:.2f}). Global supply concerns dominate."
                    else:
                        return f"Normal Brent-WTI spread (${spread:.2f}). Market in equilibrium."

            elif category == 'Feed':
                corn = prices_dict.get('Corn', 'N/A')
                soybean = prices_dict.get('Soybean', 'N/A')
                
                if corn != 'N/A' and soybean != 'N/A':
                    return f"Feed costs trending within seasonal ranges. Monitor weather impacts."

            elif category == 'Metals':
                gold = prices_dict.get('Gold', 'N/A')
                silver = prices_dict.get('Silver', 'N/A')
                
                if gold != 'N/A':
                    if gold > 2000:
                        return "Gold at premium levels. Safe-haven demand strong."
                    else:
                        return "Gold trading below key $2000 level. Monitor Fed policy."

            elif category == 'Crypto':
                btc = prices_dict.get('Bitcoin', 'N/A')
                
                if btc != 'N/A':
                    if btc > 40000:
                        return "BTC maintaining strength above 40K. Institutional interest remains."
                    else:
                        return "BTC below 40K threshold. Market sentiment cautious."

            return "Insufficient data for detailed analysis"

        except Exception as e:
            logging.error(f"Error in analysis for {category}: {e}")
            return "Analysis unavailable"

    def fetch_commodity_prices(self):
        """Fetch current prices and add analysis."""
        results = {}
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        for category, symbols_dict in self.symbols.items():
            category_data = {'Date': current_date}
            price_data = {}
            
            for symbol, display_name in symbols_dict.items():
                try:
                    time.sleep(1)
                    
                    stock = yf.Ticker(symbol)
                    history = stock.history(period="1d")
                    
                    if not history.empty:
                        current_price = history['Close'].iloc[-1]
                        category_data[display_name] = round(current_price, 4)
                        price_data[display_name] = round(current_price, 4)
                        logging.info(f"Successfully fetched data for {display_name}")
                    else:
                        logging.warning(f"No data retrieved for {display_name}")
                        category_data[display_name] = 'N/A'
                        price_data[display_name] = 'N/A'
                
                except Exception as e:
                    logging.error(f"Error fetching data for {display_name}: {e}")
                    category_data[display_name] = 'N/A'
                    price_data[display_name] = 'N/A'
            
            # Add analysis as the last column
            category_data['Market Analysis'] = self.analyze_category(category, price_data)
            results[category] = [category_data]
        
        return results

    def update_google_sheet(self, data):
        """Update Google Sheet with prices and analysis."""
        try:
            sheet = self.client.open('Commodity Price Tracker')
            
            for category, prices in data.items():
                try:
                    try:
                        worksheet = sheet.worksheet(category)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sheet.add_worksheet(category, 1000, 20)
                        logging.info(f"Created new worksheet for {category}")
                    
                    worksheet.clear()
                    
                    if prices and prices[0]:
                        headers = list(prices[0].keys())
                        worksheet.insert_row(headers, 1)
                        
                        # Format headers
                        worksheet.format('A1:Z1', {
                            'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8},
                            'textFormat': {'bold': True}
                        })
                        
                        # Add data rows
                        for row in prices:
                            worksheet.append_row(list(row.values()))
                        
                        # Adjust column widths for better readability
                        worksheet.columns_auto_resize(0, len(headers))
                        
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
        logging.info("Initializing Commodity Price Tracker")
        tracker = CommodityPriceTracker()
        tracker.run()
    except Exception as e:
        logging.critical(f"Failed to initialize or run tracker: {e}")
        raise

if __name__ == "__main__":
    main()
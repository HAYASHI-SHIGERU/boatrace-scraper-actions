import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import os
import time
import random
import sys

# Ensure we can import from utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
try:
    from utils.google_sheets import GoogleSheetsClient
except ImportError:
    from scripts.utils.google_sheets import GoogleSheetsClient

class OddsCollector:
    def __init__(self, date_str, to_sheet=False):
        self.date_str = date_str
        self.to_sheet = to_sheet
        self.base_url = "https://www.boatrace.jp/owpc/pc/race"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        self.sheet_client = None
        if self.to_sheet:
            try:
                self.sheet_client = GoogleSheetsClient()
            except Exception as e:
                print(f"Warning: Failed to initialize Google Sheets Client: {e}")
                self.to_sheet = False

    def get_active_stadiums(self):
        url = f"{self.base_url}/index?hd={self.date_str}"
        print(f"開催情報を取得中: {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            stadiums = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                if 'jcd=' in href:
                    try:
                        parts = href.split('?')
                        if len(parts) > 1:
                            params = parts[1].split('&')
                            for p in params:
                                if p.startswith('jcd='):
                                    jcd = p.split('=')[1]
                                    stadiums.add(jcd)
                    except: continue
            return sorted(list(stadiums))
        except Exception as e:
            print(f"開催情報の取得エラー: {e}")
            return []

    def scrape_odds(self, jcd, rno):
        url = f"{self.base_url}/oddstf?rno={rno}&jcd={jcd}&hd={self.date_str}"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code != 200:
                print(f"  取得失敗 (HTTP {response.status_code})")
                return []
            
            dfs = pd.read_html(response.content)
            odds_data = []
            
            win_odds_df = None
            place_odds_df = None
            
            for df in dfs:
                cols = [str(c) for c in df.columns]
                if any('単勝オッズ' in c for c in cols) and any('ボートレーサー' in c for c in cols):
                    win_odds_df = df
                elif any('複勝オッズ' in c for c in cols) and any('ボートレーサー' in c for c in cols):
                    place_odds_df = df
            
            if win_odds_df is not None and place_odds_df is not None:
                for idx in range(min(len(win_odds_df), len(place_odds_df))):
                    try:
                        boat_no = int(win_odds_df.iloc[idx, 0])
                        racer_name = str(win_odds_df.iloc[idx, 1]).strip()
                        
                        # Handle Win Odds
                        win_odds_raw = str(win_odds_df.iloc[idx, 2]).strip()
                        try:
                            if win_odds_raw in ["-", "欠場", "特払い"]:
                                win_odds = 0.0
                            else:
                                win_odds = float(win_odds_raw)
                        except (ValueError, TypeError):
                            print(f"  単勝オッズパース失敗 (R{rno} 艇{boat_no}): {win_odds_raw}")
                            win_odds = 0.0
                        
                        # Handle Place Odds
                        place_odds_str = str(place_odds_df.iloc[idx, 2]).strip()
                        try:
                            if place_odds_str in ["-", "欠場", "特払い"]:
                                place_min = place_max = 0.0
                            elif '-' in place_odds_str:
                                parts = place_odds_str.split('-')
                                place_min = float(parts[0])
                                place_max = float(parts[1])
                            else:
                                place_min = place_max = float(place_odds_str)
                        except (ValueError, TypeError):
                            place_min = place_max = 0.0
                        
                        odds_data.append({
                            'date': self.date_str,
                            'stadium_code': jcd,
                            'race_no': rno,
                            'boat_no': boat_no,
                            'racer_name': racer_name,
                            'win_odds': win_odds,
                            'place_odds_min': place_min,
                            'place_odds_max': place_max
                        })
                    except: continue
            
            return odds_data
        except Exception as e:
            print(f"  オッズ取得エラー (場:{jcd} R{rno}): {e}")
            return []

    def run(self):
        stadiums = self.get_active_stadiums()
        if not stadiums:
            return
        
        all_odds = []
        for jcd in stadiums:
            print(f"\n処理中: レース場 {jcd}")
            for rno in range(1, 13):
                odds_data = self.scrape_odds(jcd, rno)
                if odds_data:
                    all_odds.extend(odds_data)
                time.sleep(random.uniform(0.5, 1.5))
        
        if self.to_sheet and self.sheet_client and all_odds:
            df = pd.DataFrame(all_odds)
            df['date'] = df['date'].astype(str)
            self.sheet_client.write_dataframe('race_odds', df, append=True)
            print(f"Saved {len(all_odds)} rows to Sheet 'race_odds'")
        else:
            print(f"Collected {len(all_odds)} rows (Save skipped)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, required=True)
    parser.add_argument('--to-sheet', action='store_true')
    args = parser.parse_args()
    
    collector = OddsCollector(args.date, to_sheet=args.to_sheet)
    collector.run()

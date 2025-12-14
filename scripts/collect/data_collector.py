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
    # For local testing if structure is different
    from scripts.utils.google_sheets import GoogleSheetsClient

class BoatRaceScraper:
    # レース場コードと名前のマッピング
    STADIUM_NAMES = {
        '01': '桐生', '02': '戸田', '03': '江戸川', '04': '平和島',
        '05': '多摩川', '06': '浜名湖', '07': '蒲郡', '08': '常滑',
        '09': '津', '10': '三国', '11': 'びわこ', '12': '住之江',
        '13': '尼崎', '14': '鳴門', '15': '丸亀', '16': '児島',
        '17': '宮島', '18': '徳山', '19': '下関', '20': '若松',
        '21': '芦屋', '22': '福岡', '23': '唐津', '24': '大村'
    }
    
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
                print("Google Sheets Client initialized.")
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
                    except:
                        continue
            sorted_stadiums = sorted(list(stadiums))
            print(f"開催レース場: {sorted_stadiums}")
            return sorted_stadiums
        except Exception as e:
            print(f"開催情報の取得エラー: {e}")
            return []

    def scrape_race(self, jcd, rno):
        url = f"{self.base_url}/raceresult?rno={rno}&jcd={jcd}&hd={self.date_str}"
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code == 200:
                    break
                time.sleep(2)
            except requests.exceptions.RequestException:
                time.sleep(2)
        else:
            return [], []

        try:
            dfs = pd.read_html(response.content)
            results = []
            payouts = []
            
            for df in dfs:
                if isinstance(df.columns, pd.MultiIndex):
                    cols = [''.join(map(str, c)) for c in df.columns]
                else:
                    cols = [str(c) for c in df.columns]
                
                needed_cols = ['着', '枠', 'ボートレーサー', 'レースタイム']
                if all(any(n in c for c in cols) for n in needed_cols) and not results:
                    for _, row in df.iterrows():
                        try:
                            if len(row) < 4: continue
                            rank_val = row.iloc[0]
                            boat_val = row.iloc[1]
                            racer_val = row.iloc[2]
                            time_val = row.iloc[3]
                            rank_str = str(rank_val).strip()
                            if rank_str.isdigit() or (len(rank_str)==1 and ord('０') <= ord(rank_str) <= ord('９')):
                                results.append({
                                    'date': self.date_str,
                                    'stadium_code': jcd,
                                    'stadium_name': self.STADIUM_NAMES.get(jcd, '不明'),
                                    'race_no': rno,
                                    'rank': int(rank_str),
                                    'boat_no': int(str(boat_val).strip()),
                                    'racer_name': str(racer_val).strip(),
                                    'time': str(time_val).strip(),
                                    # 'win_odds': None 
                                })
                        except: continue
                
                elif any('勝式' in c for c in cols) and any('払戻金' in c for c in cols):
                    for _, row in df.iterrows():
                        try:
                            bet_type = str(row.iloc[0]).strip()
                            combination = str(row.iloc[1]).strip()
                            payout_str = str(row.iloc[2]).strip()
                            popularity = str(row.iloc[3]).strip() if len(row) > 3 else ''
                            if bet_type and bet_type != 'nan':
                                payouts.append({
                                    'date': self.date_str,
                                    'stadium_code': jcd,
                                    'stadium_name': self.STADIUM_NAMES.get(jcd, '不明'),
                                    'race_no': rno,
                                    'bet_type': bet_type,
                                    'combination': combination,
                                    'payout': int(payout_str.replace('¥', '').replace(',', '')) if '¥' in payout_str else 0,
                                    'popularity': int(float(popularity)) if popularity and popularity != 'nan' else None
                                })
                        except: continue
            return results, payouts
        except Exception as e:
            print(f"Error scraping {jcd} R{rno}: {e}")
            return [], []

    def run(self):
        stadiums = self.get_active_stadiums()
        if not stadiums:
            return

        all_results = []
        all_payouts = []
        
        for jcd in stadiums:
            print(f"処理中: レース場 {jcd}")
            for rno in range(1, 13):
                race_data, payout_data = self.scrape_race(jcd, rno)
                if race_data:
                    all_results.extend(race_data)
                if payout_data:
                    all_payouts.extend(payout_data)
                time.sleep(random.uniform(1.0, 3.0))
        
        if self.to_sheet and self.sheet_client:
            if all_results:
                df_res = pd.DataFrame(all_results)
                # Convert date to string explicitly if needed
                df_res['date'] = df_res['date'].astype(str)
                self.sheet_client.write_dataframe('race_results', df_res, append=True)
            if all_payouts:
                df_pay = pd.DataFrame(all_payouts)
                df_pay['date'] = df_pay['date'].astype(str)
                self.sheet_client.write_dataframe('race_payouts', df_pay, append=True)
            print("スプレッドシートへの保存完了")
        else:
            print(f"取得件数: 結果{len(all_results)}件, 払戻{len(all_payouts)}件 (保存スキップ)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, required=True)
    parser.add_argument('--to-sheet', action='store_true')
    args = parser.parse_args()
    
    scraper = BoatRaceScraper(args.date, to_sheet=args.to_sheet)
    scraper.run()

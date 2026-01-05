import akshare as ak
import pandas as pd
import sqlite3
import time
import random
import datetime
from tqdm import tqdm

DB_PATH = "stock_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_stock_list():
    """
    Step 1: Get all SSE/SZSE stocks and save to stock_basic table.
    Now includes FUNDAMENTAL data: PE (TTM) and Total Market Value.
    """
    print("Fetching stock list with fundamentals from AkShare...")
    try:
        # Get A-share list (real-time data)
        stock_df = ak.stock_zh_a_spot_em()
        
        # Filter for SSE (60, 68) and SZSE (00, 30) stocks
        all_stocks = stock_df[stock_df['代码'].str.startswith(('60', '68', '00', '30'))].copy()
        
        conn = get_db_connection()
        cursor = conn.cursor()

        # Re-create table with new columns
        cursor.execute("DROP TABLE IF EXISTS stock_basic")
        cursor.execute('''
            CREATE TABLE stock_basic (
                code TEXT PRIMARY KEY,
                name TEXT,
                sector TEXT,
                industry TEXT,
                float_mv REAL,
                total_mv REAL,
                pe_ttm REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # --- Fetch Industry Data (This takes a bit of time) ---
        print("Fetching industry mapping (this may take 1-2 minutes)...")
        code_to_industry = {}
        try:
            # Get list of all industries
            ind_list_df = ak.stock_board_industry_name_em()
            # Iterate through each industry to get member stocks
            # Optimize: use tqdm if possible, or just print progress
            for idx, row in ind_list_df.iterrows():
                ind_name = row['板块名称']
                try:
                    # Get members of this industry
                    members = ak.stock_board_industry_cons_em(symbol=ind_name)
                    for _, m_row in members.iterrows():
                        m_code = m_row['代码']
                        code_to_industry[m_code] = ind_name
                except:
                    continue
                if idx % 10 == 0: print(f"Processed industry: {ind_name}")
        except Exception as e:
            print(f"Warning: Failed to fetch industry data: {e}")

        # Preparing data for insertion
        data_to_insert = []
        for _, row in all_stocks.iterrows():
            code = row['代码']
            name = row['名称']
            
            # Fundamentals
            try:
                float_mv = float(row['流通市值']) if row['流通市值'] else 0
                total_mv = float(row['总市值']) if row['总市值'] else 0
                pe_ttm = float(row['市盈率-动态']) if row['市盈率-动态'] else 0
            except:
                float_mv = 0; total_mv = 0; pe_ttm = 0
            
            # Sector (Market Board)
            if code.startswith('60'): sector = "Main Board"
            elif code.startswith('68'): sector = "STAR Market"
            elif code.startswith('00'): sector = "SZSE Main Board"
            elif code.startswith('30'): sector = "ChiNext"
            else: sector = "Other"
            
            # Industry (Concept/Sector)
            industry = code_to_industry.get(code, "Unknown")
            
            data_to_insert.append((code, name, sector, industry, float_mv, total_mv, pe_ttm))
        
        # Bulk insert
        cursor.executemany('''
            INSERT INTO stock_basic (code, name, sector, industry, float_mv, total_mv, pe_ttm)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', data_to_insert)
        
        conn.commit()
        conn.close()
        print(f"Successfully updated stock_basic (with Fundamentals) for {len(data_to_insert)} stocks.")
        return [x[0] for x in data_to_insert]
        
    except Exception as e:
        print(f"Error initializing stock list: {e}")
        return []

def download_daily_data(stock_codes, default_start_date="20250101"):
    """
    Step 2: Download daily OHLCV for ALL stocks and Margin data for SSE (Shanghai) stocks ONLY.
    SZSE Margin data is handled separately.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total = len(stock_codes)
    print(f"Starting OHLCV & SSE Margin update for {total} stocks...")
    
    # Pre-fetch latest dates to minimize DB queries in the loop
    cursor.execute("SELECT code, MAX(trade_date) FROM daily_market GROUP BY code")
    latest_market_dates = dict(cursor.fetchall())

    # NOTE: daily_market may update earlier than margin_data. If we use daily's latest date
    # as the start_date for margin as well, SSE margin can get stuck forever (never catch up).
    cursor.execute(
        "SELECT code, MAX(trade_date) FROM margin_data "
        "WHERE code LIKE '60%' OR code LIKE '68%' GROUP BY code"
    )
    latest_sse_margin_dates = dict(cursor.fetchall())

    for i, code in enumerate(stock_codes):
        try:
            now = datetime.datetime.now()
            today_str = now.strftime("%Y%m%d")

            # Determine daily_market start date
            daily_start_date = None
            last_market_date_str = latest_market_dates.get(code)
            if last_market_date_str:
                last_market_date = datetime.datetime.strptime(last_market_date_str, "%Y-%m-%d")
                next_day = last_market_date + datetime.timedelta(days=1)
                if next_day <= now:
                    daily_start_date = next_day.strftime("%Y%m%d")
            else:
                daily_start_date = default_start_date

            need_daily = bool(daily_start_date and daily_start_date <= today_str)

            # Determine SSE margin_data start date (independent from daily_market)
            margin_start_date = None
            need_margin = False
            if code.startswith(("60", "68")):
                last_margin_date_str = latest_sse_margin_dates.get(code)
                if last_margin_date_str:
                    last_margin_date = datetime.datetime.strptime(last_margin_date_str, "%Y-%m-%d")
                    margin_next_day = last_margin_date + datetime.timedelta(days=1)
                    if margin_next_day <= now:
                        margin_start_date = margin_next_day.strftime("%Y%m%d")
                else:
                    margin_start_date = default_start_date

                need_margin = bool(margin_start_date and margin_start_date <= today_str)

            if not need_daily and not need_margin:
                continue

            # -----------------------------------------------
            # A. Get Daily Market Data (OHLCV + Turnover)
            # -----------------------------------------------
            if need_daily:
                stock_daily = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=daily_start_date, adjust="qfq")
                
                if not stock_daily.empty:
                    daily_records = []
                    for _, row in stock_daily.iterrows():
                        date_str = row['日期']
                        daily_records.append((
                            code, date_str, 
                            row['开盘'], row['最高'], row['最低'], row['收盘'], 
                            row['成交量'], row['换手率']
                        ))
                    
                    cursor.executemany('''
                        INSERT OR REPLACE INTO daily_market (code, trade_date, open, high, low, close, volume, turnover_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', daily_records)

            # -----------------------------------------------
            # B. Get Margin Data (SSE ONLY)
            # -----------------------------------------------
            if need_margin:
                try:
                    end_date = today_str
                    margin_df = ak.stock_margin_detail_sse(symbol=code, start_date=margin_start_date, end_date=end_date)
                    
                    if not margin_df.empty:
                        margin_records = []
                        for _, row in margin_df.iterrows():
                            m_date = row['信用交易日期']
                            f_bal = row['融资余额']
                            f_buy = row['融资买入额']
                            f_repay = row.get('融资偿还额', 0)
                            s_bal = row['融券余量']
                            s_sell = row['融券卖出量']
                            net_buy = float(f_buy) - float(f_repay)

                            margin_records.append((
                                code, m_date,
                                f_buy, f_bal, s_sell, s_bal, net_buy
                            ))
                        
                        cursor.executemany('''
                            INSERT OR REPLACE INTO margin_data 
                            (code, trade_date, financing_buy, financing_balance, securities_sell, securities_balance, net_financing_buy)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', margin_records)
                except Exception:
                    pass

            conn.commit()
            
            if (i + 1) % 50 == 0:
                print(f"[{i+1}/{total}] Processed {code}")
            
        except Exception as e:
            print(f"Error processing {code}: {e}")
            continue

    conn.close()
    print("Daily OHLCV & SSE Margin update complete.")

def download_szse_margin_data(start_date="20250101"):
    """
    Step 2.5: Download SZSE Margin Data by DATE (Batch Mode).
    SZSE interface supports querying by date for all stocks, which is much faster/correct.
    """
    print(f"Starting SZSE Margin Data download from {start_date}...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.datetime.now()
    
    # Cache last balance to calculate Net Buy (since SZSE doesn't provide Repay/Net data directly)
    # Net Buy ~= Balance_Today - Balance_Yesterday
    last_balances = {} 
    
    # Try to load latest balances from DB to initialize cache
    try:
        # Get the latest available date in DB for SZSE stocks
        cursor.execute("SELECT code, financing_balance FROM margin_data WHERE code LIKE '00%' OR code LIKE '30%'")
        # This is a bit heavy, but needed for incremental accuracy. 
        # Ideally we only need the balance of the day BEFORE start_date.
        # Simplification: We will just fetch the latest record for each stock.
        # Actually, SQL to get "latest record per group" is complex.
        # Let's assume if we run this, we run continuously. 
        # For now, start with empty cache. The first day's Net Buy might be inaccurate (0), but subsequent days will be correct.
        pass
    except:
        pass

    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y%m%d")
        db_date_str = current_dt.strftime("%Y-%m-%d")
        
        # Check if already exists? (Optional, but good for skipping)
        # But since we are doing batch, maybe just overwrite.
        
        try:
            # Fetch SZSE Margin for specific date
            margin_df = ak.stock_margin_detail_szse(date=date_str)
            
            if not margin_df.empty:
                # Columns: 证券代码, 证券简称, 融资买入额, 融资余额, 融券卖出量, 融券余量, 融券余额, 融资融券余额
                records = []
                for _, row in margin_df.iterrows():
                    code = str(row['证券代码'])
                    f_buy = float(row['融资买入额'])
                    f_bal = float(row['融资余额'])
                    s_sell = float(row['融券卖出量'])
                    s_bal = float(row['融券余量'])
                    
                    # Calculate Net Buy
                    # If we have previous balance, Net = Bal_Now - Bal_Prev
                    # If not, Net = 0 (or F_Buy, but that's wrong). Let's use 0 for the first day we see it.
                    prev_bal = last_balances.get(code, f_bal) 
                    net_buy = f_bal - prev_bal
                    
                    # Update cache
                    last_balances[code] = f_bal
                    
                    records.append((
                        code, db_date_str,
                        f_buy, f_bal, s_sell, s_bal, net_buy
                    ))
                
                cursor.executemany('''
                    INSERT OR REPLACE INTO margin_data 
                    (code, trade_date, financing_buy, financing_balance, securities_sell, securities_balance, net_financing_buy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', records)
                conn.commit()
                print(f"[SZSE Margin] Updated {len(records)} records for {date_str}")
            else:
                print(f"[SZSE Margin] No data for {date_str} (Holiday?)")
                
        except Exception as e:
            print(f"Error fetching SZSE margin for {date_str}: {e}")
            
        current_dt += datetime.timedelta(days=1)
        time.sleep(0.5)

    conn.close()
    print("SZSE Margin update complete.")

def download_northbound_data(start_date="20250101"):
    """
    Step 3: Download Northbound (沪股通/深股通) holding data.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Starting Northbound funds data download from {start_date}...")
    
    start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.datetime.now()
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y%m%d")
        
        check_sql = f"SELECT 1 FROM northbound_data WHERE trade_date = '{current_dt.strftime('%Y-%m-%d')}' LIMIT 1"
        cursor.execute(check_sql)
        if cursor.fetchone():
            current_dt += datetime.timedelta(days=1)
            continue

        try:
            for market in ["沪股通", "深股通"]:
                nb_df = ak.stock_hsgt_hold_stock_em(market=market, date=date_str)
                if not nb_df.empty:
                    records = []
                    db_date = current_dt.strftime("%Y-%m-%d")
                    for _, row in nb_df.iterrows():
                        code = row['代码']
                        records.append((code, db_date, row['持股市值']))

                    cursor.executemany('''
                        INSERT OR REPLACE INTO northbound_data (code, trade_date, net_inflow)
                        VALUES (?, ?, ?)
                    ''', records)
                time.sleep(0.5)
            conn.commit()
            print(f"[Northbound] Downloaded data for {date_str}")
        except Exception as e:
            pass
            
        current_dt += datetime.timedelta(days=1)

    conn.close()
    print("Northbound data download complete.")

def init_tables():
    """Ensure all data tables exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_market (
            code TEXT,
            trade_date DATE,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            turnover_rate REAL,
            PRIMARY KEY (code, trade_date)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS margin_data (
            code TEXT,
            trade_date DATE,
            financing_buy REAL,
            financing_balance REAL,
            securities_sell REAL,
            securities_balance REAL,
            net_financing_buy REAL,
            PRIMARY KEY (code, trade_date)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS northbound_data (
            code TEXT,
            trade_date DATE,
            net_inflow REAL,
            PRIMARY KEY (code, trade_date)
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_tables()
    codes = init_stock_list()
    
    # 1. Standard OHLCV + SSE Margin
    download_daily_data(codes, default_start_date="20250101") 
    
    # 2. SZSE Margin (New Logic)
    download_szse_margin_data(start_date="20250101")
    
    # 3. Northbound
    download_northbound_data(start_date="20250101")

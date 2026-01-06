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
    Step 2: Download daily OHLCV for ALL stocks.
    Margin (SSE/SZSE) and Northbound are handled in separate steps.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total = len(stock_codes)
    print(f"Starting OHLCV update for {total} stocks...")
    
    # Pre-fetch latest dates to minimize DB queries in the loop
    cursor.execute("SELECT code, MAX(trade_date) FROM daily_market GROUP BY code")
    latest_market_dates = dict(cursor.fetchall())

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

            if not need_daily:
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

            conn.commit()
            
            if (i + 1) % 50 == 0:
                print(f"[{i+1}/{total}] Processed {code}")
            
        except Exception as e:
            print(f"Error processing {code}: {e}")
            continue

    conn.close()
    print("Daily OHLCV update complete.")

def download_sse_margin_data(start_date="20250101"):
    """
    Step 2.3: Download SSE Margin Data by DATE (Batch Mode).
    """
    print(f"Starting SSE Margin Data download (incremental, default start={start_date})...")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Determine target end date from daily_market to align calendars
    cursor.execute("SELECT MAX(trade_date) FROM daily_market")
    end_date_db = cursor.fetchone()[0]
    if not end_date_db:
        conn.close()
        print("[SSE Margin] No daily_market data; skip.")
        return

    # Determine last updated SSE margin date
    cursor.execute("SELECT MAX(trade_date) FROM margin_data WHERE code LIKE '60%' OR code LIKE '68%'")
    last_date_db = cursor.fetchone()[0]

    if last_date_db:
        # Only fetch dates AFTER last_date_db (use daily_market trading dates to skip holidays)
        cursor.execute(
            "SELECT trade_date FROM daily_market "
            "WHERE trade_date > ? AND trade_date <= ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (last_date_db, end_date_db),
        )
    else:
        start_date_db = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
        cursor.execute(
            "SELECT trade_date FROM daily_market "
            "WHERE trade_date >= ? AND trade_date <= ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (start_date_db, end_date_db),
        )

    trade_dates = [r[0] for r in cursor.fetchall()]
    if not trade_dates:
        conn.close()
        print("[SSE Margin] Up to date.")
        return

    for trade_date in trade_dates:
        date_str = trade_date.replace("-", "")
        try:
            margin_df = ak.stock_margin_detail_sse(date=date_str)
        except Exception as e:
            print(f"[SSE Margin] Error fetching {date_str}: {e}")
            continue

        if margin_df.empty:
            print(f"[SSE Margin] No data for {date_str} (Holiday?)")
            continue

        records = []
        for _, row in margin_df.iterrows():
            code = str(row["标的证券代码"]).zfill(6)
            m_date = row["信用交易日期"]
            f_buy = float(row["融资买入额"])
            f_bal = float(row["融资余额"])
            f_repay = float(row.get("融资偿还额", 0) or 0)
            s_sell = float(row["融券卖出量"])
            s_bal = float(row["融券余量"])
            net_buy = f_buy - f_repay

            records.append((code, m_date, f_buy, f_bal, s_sell, s_bal, net_buy))

        cursor.executemany(
            """
            INSERT OR REPLACE INTO margin_data
            (code, trade_date, financing_buy, financing_balance, securities_sell, securities_balance, net_financing_buy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        conn.commit()
        print(f"[SSE Margin] Updated {len(records)} records for {date_str}")

    conn.close()
    print("SSE Margin update complete.")

def download_szse_margin_data(start_date="20250101"):
    """
    Step 2.5: Download SZSE Margin Data by DATE (Batch Mode).
    SZSE interface supports querying by date for all stocks, which is much faster/correct.
    """
    print(f"Starting SZSE Margin Data download (incremental, default start={start_date})...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Determine target end date from daily_market to align calendars
    cursor.execute("SELECT MAX(trade_date) FROM daily_market")
    end_date_db = cursor.fetchone()[0]
    if not end_date_db:
        conn.close()
        print("[SZSE Margin] No daily_market data; skip.")
        return

    # Determine last updated SZSE margin date
    cursor.execute("SELECT MAX(trade_date) FROM margin_data WHERE code LIKE '00%' OR code LIKE '30%'")
    last_date_db = cursor.fetchone()[0]

    # Cache last balance to calculate Net Buy (since SZSE doesn't provide Repay/Net data directly)
    # Net Buy ~= Balance_Today - Balance_PrevTradeDate
    last_balances = {}

    if last_date_db:
        # Initialize cache from last available date in DB
        try:
            cursor.execute(
                "SELECT code, financing_balance FROM margin_data "
                "WHERE trade_date = ? AND (code LIKE '00%' OR code LIKE '30%')",
                (last_date_db,),
            )
            last_balances = {code: float(bal) for code, bal in cursor.fetchall()}
        except Exception:
            last_balances = {}

        cursor.execute(
            "SELECT trade_date FROM daily_market "
            "WHERE trade_date > ? AND trade_date <= ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (last_date_db, end_date_db),
        )
    else:
        start_date_db = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
        cursor.execute(
            "SELECT trade_date FROM daily_market "
            "WHERE trade_date >= ? AND trade_date <= ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (start_date_db, end_date_db),
        )

    trade_dates = [r[0] for r in cursor.fetchall()]
    if not trade_dates:
        conn.close()
        print("[SZSE Margin] Up to date.")
        return

    for trade_date in trade_dates:
        date_str = trade_date.replace("-", "")
        margin_df = None
        for attempt in range(1, 4):
            try:
                margin_df = ak.stock_margin_detail_szse(date=date_str)
                break
            except Exception as e:
                if attempt >= 3:
                    print(f"[SZSE Margin] Error fetching {date_str}: {e}")
                else:
                    wait_s = (2 ** (attempt - 1)) + random.random() * 0.5
                    print(f"[SZSE Margin] Error fetching {date_str} (attempt {attempt}/3): {e}; retry in {wait_s:.1f}s")
                    time.sleep(wait_s)
        if margin_df is None:
            continue

        if margin_df.empty:
            print(f"[SZSE Margin] No data for {date_str} (Holiday?)")
            continue

        records = []
        for _, row in margin_df.iterrows():
            code = str(row["证券代码"]).zfill(6)
            f_buy = float(row["融资买入额"])
            f_bal = float(row["融资余额"])
            s_sell = float(row["融券卖出量"])
            s_bal = float(row["融券余量"])

            prev_bal = last_balances.get(code, f_bal)
            net_buy = f_bal - prev_bal
            last_balances[code] = f_bal

            records.append((code, trade_date, f_buy, f_bal, s_sell, s_bal, net_buy))

        cursor.executemany(
            """
            INSERT OR REPLACE INTO margin_data
            (code, trade_date, financing_buy, financing_balance, securities_sell, securities_balance, net_financing_buy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        conn.commit()
        print(f"[SZSE Margin] Updated {len(records)} records for {date_str}")

    conn.close()
    print("SZSE Margin update complete.")

def download_northbound_data(start_date="20250101"):
    """
    Step 3: Download Northbound (沪股通/深股通) holding data.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Starting Northbound (holdings) data download (incremental, default start={start_date})...")

    # Determine target end date from daily_market to align calendars
    cursor.execute("SELECT MAX(trade_date) FROM daily_market")
    end_date_db = cursor.fetchone()[0]
    if not end_date_db:
        conn.close()
        print("[Northbound] No daily_market data; skip.")
        return

    # Determine last updated date in DB
    cursor.execute("SELECT MAX(trade_date) FROM northbound_data")
    last_date_db = cursor.fetchone()[0]

    # Re-download the last available day as well to repair partial data.
    if last_date_db:
        cursor.execute(
            "SELECT trade_date FROM daily_market "
            "WHERE trade_date >= ? AND trade_date <= ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (last_date_db, end_date_db),
        )
    else:
        start_date_db = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
        cursor.execute(
            "SELECT trade_date FROM daily_market "
            "WHERE trade_date >= ? AND trade_date <= ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (start_date_db, end_date_db),
        )

    trade_dates = [r[0] for r in cursor.fetchall()]
    if not trade_dates:
        conn.close()
        print("[Northbound] Up to date.")
        return

    for trade_date in trade_dates:
        date_str = trade_date.replace("-", "")
        try:
            target_dt = datetime.datetime.strptime(trade_date, "%Y-%m-%d").date()
        except Exception:
            print(f"[Northbound] Invalid trade_date: {trade_date}")
            continue

        # Workaround: Avoid AkShare's equality filter when start_date == end_date.
        # Use a 2-day window (target_dt - 1 day .. target_dt) then filter back to target_dt.
        # This also avoids requesting an end_date in the future (target_dt + 1 day).
        range_start = (target_dt - datetime.timedelta(days=1)).strftime("%Y%m%d")
        range_end = date_str
        frames = []

        # Use separate endpoints (HTTPS) for沪股通/深股通 to avoid the AkShare "北向持股" HTTP bug.
        for symbol in ["沪股通持股", "深股通持股"]:
            df_part = None
            for attempt in range(1, 4):
                try:
                    df_part = ak.stock_hsgt_stock_statistics_em(
                        symbol=symbol,
                        start_date=range_start,
                        end_date=range_end,
                    )
                    break
                except Exception as e:
                    if attempt >= 3:
                        print(f"[Northbound] Error fetching {symbol} {date_str}: {e}")
                    else:
                        wait_s = (2 ** (attempt - 1)) + random.random() * 0.5
                        print(f"[Northbound] Error fetching {symbol} {date_str} (attempt {attempt}/3): {e}; retry in {wait_s:.1f}s")
                        time.sleep(wait_s)

            if df_part is not None and not df_part.empty:
                try:
                    if "持股日期" in df_part.columns:
                        s_dt = pd.to_datetime(df_part["持股日期"], errors="coerce").dt.date
                        df_part = df_part[s_dt == target_dt]
                except Exception:
                    pass
                if not df_part.empty:
                    frames.append(df_part)

        if not frames:
            print(f"[Northbound] No data for {trade_date}")
            continue

        df_day = pd.concat(frames, ignore_index=True)
        try:
            df_day = df_day.dropna(subset=["股票代码", "持股市值"])
        except Exception:
            pass

        records = []
        for _, row in df_day.iterrows():
            code = str(row.get("股票代码", "")).zfill(6)
            try:
                hold_val = float(row["持股市值"])
            except Exception:
                continue
            if not code or code == "000000":
                continue
            records.append((code, trade_date, hold_val))

        if not records:
            print(f"[Northbound] No valid rows for {trade_date}")
            continue

        cursor.execute("DELETE FROM northbound_data WHERE trade_date = ?", (trade_date,))
        cursor.executemany(
            "INSERT OR REPLACE INTO northbound_data (code, trade_date, net_inflow) VALUES (?, ?, ?)",
            records,
        )
        conn.commit()
        print(f"[Northbound] Updated {len(records)} records for {trade_date}")

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
    
    # 1. Standard OHLCV
    download_daily_data(codes, default_start_date="20250101") 

    # 2. SSE Margin
    download_sse_margin_data(start_date="20250101")
    
    # 3. SZSE Margin
    download_szse_margin_data(start_date="20250101")
    
    # 4. Northbound
    download_northbound_data(start_date="20250101")

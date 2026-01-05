import pandas as pd
import sqlite3
import datetime

DB_PATH = "stock_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def get_stock_data(stock_code: str, conn):
    """
    Legacy function: Loads all relevant data for a single stock.
    Kept for 'Deep Dive' page compatibility.
    """
    # Load daily market data
    daily_df = pd.read_sql(f"SELECT * FROM daily_market WHERE code = '{stock_code}'", conn)
    if daily_df.empty:
        return pd.DataFrame(), None
    daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date'])
    daily_df = daily_df.set_index('trade_date').sort_index()

    # Load margin data
    margin_df = pd.read_sql(f"SELECT * FROM margin_data WHERE code = '{stock_code}'", conn)
    if not margin_df.empty:
        margin_df['trade_date'] = pd.to_datetime(margin_df['trade_date'])
        margin_df = margin_df.set_index('trade_date').sort_index()
        margin_df = margin_df.drop(columns=['code'], errors='ignore')

    # Load Northbound data
    try:
        nb_df = pd.read_sql(f"SELECT * FROM northbound_data WHERE code = '{stock_code}'", conn)
        if not nb_df.empty:
            nb_df['trade_date'] = pd.to_datetime(nb_df['trade_date'])
            nb_df = nb_df.set_index('trade_date').sort_index()
            nb_df = nb_df.rename(columns={'net_inflow': 'nb_hold_val'})
            nb_df = nb_df.drop(columns=['code'], errors='ignore')
    except Exception:
        nb_df = pd.DataFrame()

    # Join
    full_df = daily_df.join(margin_df, how='left')
    if not nb_df.empty:
        full_df = full_df.join(nb_df['nb_hold_val'], how='left')
    else:
        full_df['nb_hold_val'] = 0.0

    full_df = full_df.fillna(0) 

    # Load basic info
    try:
        basic_info_series = pd.read_sql(f"SELECT name, float_mv, sector, industry, total_mv, pe_ttm FROM stock_basic WHERE code = '{stock_code}'", conn).iloc[0]
        sector_map = {
            "Main Board": "沪市主板", 
            "STAR Market": "科创板",
            "SZSE Main Board": "深市主板",
            "ChiNext": "创业板"
        }
        basic_info_series['sector'] = sector_map.get(basic_info_series['sector'], basic_info_series['sector'])
    except:
        return pd.DataFrame(), None
    
    return full_df.reset_index(), basic_info_series

def get_market_sentiment(conn):
    try:
        last_date = pd.read_sql("SELECT MAX(trade_date) FROM daily_market", conn).iloc[0, 0]
        if not last_date:
            return "⚪️ 数据不足", 0, 0, ""

        df = pd.read_sql(f"SELECT close, open FROM daily_market WHERE trade_date = '{last_date}'", conn)
        
        up_count = len(df[df['close'] > df['open']])
        down_count = len(df[df['close'] < df['open']])
        total = len(df)
        
        if total == 0: return "⚪️ 无数据", 0, 0, ""
        
        up_ratio = up_count / total
        
        if up_ratio > 0.8: status = "🔥 极度狂热"
        elif up_ratio > 0.5: status = "🔴 市场温和"
        elif up_ratio > 0.2: status = "🟢 市场低迷"
        else: status = "❄️ 极度冰点"
        
        return status, up_count, down_count, last_date
    except:
        return "⚪️ 计算失败", 0, 0, ""

def check_fundamentals(info_series):
    name = info_series['name']
    total_mv = info_series.get('total_mv', 0)
    
    if 'ST' in name:
        return False, "黑名单 (ST股)"
    
    # < 20亿
    if total_mv > 0 and total_mv < 2000000000: 
        return False, "黑名单 (微盘股)"

    return True, ""

def generate_signals_row(row, info):
    """Row-wise signal generation based on Docs"""
    # 0. Fundamentals (Blacklist)
    passed, reason = check_fundamentals(info)
    if not passed:
        return f"⚫ {reason}"
        
    # 1. Extract Data
    surge = row.get('financing_surge_pct', 0)      # >0 means Net Buy
    nb_inflow = row.get('nb_inflow', 0)            # >0 means Net Buy
    net_fin = row.get('net_financing_buy', 0)      # Absolute value
    close = row.get('close', 0)
    ma20 = row.get('close_20d_avg', 0)
    
    # 2. Complex Logic (Priority Order)
    
    # [Strong Buy] Trend + Funds Resonance
    # Logic: Price > MA20 AND (Financing > 0 AND Northbound > 0)
    if close > ma20 and net_fin > 0 and nb_inflow > 0:
        return "🟢 强力买入"

    # [Watch] Bottom Fishing / Rebound
    # Logic: Price < MA20 BUT Funds are flowing in (Financing OR Northbound)
    if close < ma20 and (net_fin > 0 or nb_inflow > 0):
        return "🟡 关注(底部异动)"

    # [Stop Loss] Trend Broken + Funds Fleeing
    # Logic: Price < MA20 AND Funds Leaving
    if close < ma20 and (net_fin < 0 or nb_inflow < 0):
        return "⚫ 止损离场"

    # [Risk] Massive Outflow (regardless of trend)
    if surge < -0.003:
        return "💸 资金出逃"

    # [Trend] Simple Trend Up (No significant fund resonance)
    if close > ma20:
        return "📈 趋势向上"
         
    return "⚪️ 中性"

def get_full_analysis_report():
    """
    Optimized Bulk Loading Version.
    1. Loads Stock Basic info.
    2. Loads last ~60 days of market/margin/northbound data for all stocks.
    3. Merges and calculates indicators in memory (Vectorized).
    4. Returns latest snapshot.
    """
    conn = get_db_connection()
    
    print("Loading Basic Info...")
    # 1. Basic Info
    try:
        basic_df = pd.read_sql("SELECT * FROM stock_basic", conn)
    except:
        conn.close()
        return pd.DataFrame()
        
    basic_df = basic_df.set_index('code')
    
    # 2. Determine Date Range (Last 60 days for MAs)
    try:
        max_date_res = pd.read_sql("SELECT MAX(trade_date) as max_date FROM daily_market", conn)
        max_date_str = max_date_res.iloc[0]['max_date']
        if not max_date_str:
            conn.close()
            return pd.DataFrame()
        
        end_date = pd.to_datetime(max_date_str)
        start_date = end_date - pd.Timedelta(days=60)
        start_date_str = start_date.strftime('%Y-%m-%d')
    except:
        conn.close()
        return pd.DataFrame()

    print(f"Loading Market Data from {start_date_str}...")
    
    # 3. Bulk Load Tables
    daily_df = pd.read_sql(f"SELECT * FROM daily_market WHERE trade_date >= '{start_date_str}'", conn)
    margin_df = pd.read_sql(f"SELECT * FROM margin_data WHERE trade_date >= '{start_date_str}'", conn)
    try:
        nb_df = pd.read_sql(f"SELECT * FROM northbound_data WHERE trade_date >= '{start_date_str}'", conn)
    except:
        nb_df = pd.DataFrame()

    conn.close()
    
    if daily_df.empty:
        return pd.DataFrame()

    # 4. Pre-processing
    daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date'])
    if not margin_df.empty:
        margin_df['trade_date'] = pd.to_datetime(margin_df['trade_date'])
    if not nb_df.empty:
        nb_df['trade_date'] = pd.to_datetime(nb_df['trade_date'])
        nb_df = nb_df.rename(columns={'net_inflow': 'nb_hold_val'})

    # 5. Merging (Left Join on Code + Date)
    merged = pd.merge(daily_df, margin_df, on=['code', 'trade_date'], how='left')
    if not nb_df.empty:
        merged = pd.merge(merged, nb_df, on=['code', 'trade_date'], how='left')
    else:
        merged['nb_hold_val'] = 0.0
        
    # Fill NAs
    cols_to_fill = ['financing_buy', 'financing_balance', 'net_financing_buy', 'nb_hold_val']
    for c in cols_to_fill:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)
        else:
            merged[c] = 0.0

    # 6. Vectorized Calculations
    # Map float_mv from basic_df
    # We use map for faster lookup
    merged['float_mv'] = merged['code'].map(basic_df['float_mv'])
    
    # Financing Surge
    # Avoid division by zero
    merged['financing_surge_pct'] = 0.0
    mask_mv = (merged['float_mv'] > 0)
    merged.loc[mask_mv, 'financing_surge_pct'] = merged.loc[mask_mv, 'net_financing_buy'] / merged.loc[mask_mv, 'float_mv']

    # NB Inflow (Diff of holding value)
    merged = merged.sort_values(['code', 'trade_date'])
    merged['nb_inflow'] = merged.groupby('code')['nb_hold_val'].diff().fillna(0)
    
    # Moving Averages
    # rolling() on groupby is reasonably fast
    merged['close_20d_avg'] = merged.groupby('code')['close'].rolling(20, min_periods=1).mean().reset_index(0, drop=True)
    
    # 7. Extract Latest Snapshot
    # Get the row with max date for EACH code
    latest_snapshot = merged.groupby('code').tail(1).copy()
    
    # Need 'previous close' for Chg% calculation if not present.
    # Actually, daily_market doesn't store Chg%, so we calculate it.
    # We need the row BEFORE the last one.
    # Alternative: use pct_change() on the full set.
    merged['chg_pct'] = merged.groupby('code')['close'].pct_change() * 100
    # Update snapshot with chg_pct
    latest_snapshot['chg_pct'] = merged.loc[latest_snapshot.index, 'chg_pct'].fillna(0)

    # 8. Final Formatting & Signal Generation
    results = []
    
    # Pre-fetch sector map to avoid repeated lookups
    sector_map = {
        "Main Board": "沪市主板", "STAR Market": "科创板",
        "SZSE Main Board": "深市主板", "ChiNext": "创业板"
    }

    # Iterate over the SNAPSHOT (much smaller, ~5000 rows)
    for idx, row in latest_snapshot.iterrows():
        code = row['code']
        if code not in basic_df.index: continue
        
        info = basic_df.loc[code]
        
        # Prepare info dict for compatibility
        info_dict = {
            'name': info['name'],
            'total_mv': info['total_mv'],
            'pe_ttm': info['pe_ttm'],
            'float_mv': info['float_mv'],
            'sector': sector_map.get(info['sector'], info['sector']),
            'industry': info['industry']
        }

        # Generate Signal
        signal = generate_signals_row(row, info_dict)

        results.append({
            "Code": code,
            "Name": info_dict['name'],
            "Sector": info_dict['sector'],
            "Industry": info_dict['industry'], 
            "PE": round(info_dict.get('pe_ttm', 0), 1), 
            "Mkt Cap": round(info_dict.get('total_mv', 0) / 100000000, 1), 
            "Signal": signal,
            "Close": round(row['close'], 2),
            "Chg%": round(row['chg_pct'], 2),
            "Turnover%": round(row['turnover_rate'], 2),
            "Financing Bal": round(row['financing_balance'] / 100000000, 2), 
            "Financing Net": round(row['net_financing_buy'] / 10000, 2), 
            "Northbound Hold": round(row['nb_hold_val'] / 100000000, 2), 
            "NB Inflow": round(row['nb_inflow'] / 10000, 2), 
            "Surge Score": round(row['financing_surge_pct'] * 1000, 2) 
        })
        
    return pd.DataFrame(results)

if __name__ == '__main__':
    start_t = datetime.datetime.now()
    df = get_full_analysis_report()
    end_t = datetime.datetime.now()
    print(df.head())
    print(f"Generated report for {len(df)} stocks in {(end_t - start_t).total_seconds():.2f} seconds.")
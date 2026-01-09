import pandas as pd
import datetime
import db

def get_db_connection():
    return db.get_db_connection()

def get_latest_common_trade_date(conn):
    """
    Returns the latest common trade_date across daily/margin/northbound tables.

    daily_market often updates earlier than margin/northbound. If we build the report on a
    date that exists only in daily_market, financing/northbound columns will be NaN and
    later filled as 0, making the dashboard "资金(融/北)" column look empty.
    """
    tables = ["daily_market", "margin_data", "northbound_data", "main_fund_flow"]
    max_dates = []

    for table in tables:
        try:
            max_date = pd.read_sql(f"SELECT MAX(trade_date) AS max_date FROM {table}", conn).iloc[0, 0]
        except Exception:
            max_date = None

        if max_date:
            max_dates.append(pd.to_datetime(max_date))

    if not max_dates:
        return None

    return min(max_dates).strftime("%Y-%m-%d")

def get_table_max_dates(conn):
    """Return MAX(trade_date) for each core table."""
    tables = ["daily_market", "margin_data", "northbound_data", "main_fund_flow"]
    result = {}

    for table in tables:
        try:
            max_date = pd.read_sql(f"SELECT MAX(trade_date) AS max_date FROM {table}", conn).iloc[0, 0]
        except Exception:
            max_date = None
        result[table] = max_date

    return result

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
        last_date = pd.read_sql("SELECT MAX(trade_date) AS max_date FROM daily_market", conn).iloc[0, 0]
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

def get_signals_for_codes(conn, codes, lookback_days: int = 60):
    """
    Return latest Signal for a subset of codes (used by Portfolio page).

    This avoids loading the full-market analysis report just to compute signals for a
    small holdings list.
    """
    if not codes:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    cleaned = []
    for c in codes:
        if c is None:
            continue
        s = str(c).strip()
        if not s:
            continue
        if s.isdigit():
            cleaned.append(s.zfill(6))
            continue
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == 6:
            cleaned.append(digits)

    codes = sorted(set(cleaned))
    if not codes:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    code_list_sql = ",".join([f"'{c}'" for c in codes])

    # Basic info
    try:
        basic_df = pd.read_sql(
            f"SELECT code, name, sector, industry, total_mv, float_mv, pe_ttm FROM stock_basic "
            f"WHERE code IN ({code_list_sql})",
            conn,
        )
    except Exception:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    if basic_df.empty:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    basic_df = basic_df.set_index("code")

    # Date range from daily_market (for MA20 window)
    try:
        max_date_res = pd.read_sql("SELECT MAX(trade_date) as max_date FROM daily_market", conn)
        end_date_str = max_date_res.iloc[0]["max_date"]
    except Exception:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    if not end_date_str:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    end_date = pd.to_datetime(end_date_str)
    start_date = end_date - pd.Timedelta(days=int(lookback_days or 60))
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Daily (close + MA20)
    daily_df = pd.read_sql(
        f"SELECT code, trade_date, close FROM daily_market "
        f"WHERE code IN ({code_list_sql}) "
        f"AND trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
        conn,
    )
    if daily_df.empty:
        return pd.DataFrame(columns=["Code", "Signal", "Surge Score"])

    daily_df["trade_date"] = pd.to_datetime(daily_df["trade_date"])
    daily_df = daily_df.sort_values(["code", "trade_date"])
    daily_df["close_20d_avg"] = (
        daily_df.groupby("code")["close"]
        .rolling(20, min_periods=1)
        .mean()
        .reset_index(0, drop=True)
    )
    latest = daily_df.groupby("code").tail(1).set_index("code")

    # Latest financing (may lag daily date; join by code)
    try:
        margin_df = pd.read_sql(
            f"SELECT code, trade_date, net_financing_buy FROM margin_data "
            f"WHERE code IN ({code_list_sql}) "
            f"AND trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
            conn,
        )
    except Exception:
        margin_df = pd.DataFrame()

    if not margin_df.empty:
        margin_df["trade_date"] = pd.to_datetime(margin_df["trade_date"])
        margin_latest = (
            margin_df.sort_values(["code", "trade_date"]).groupby("code").tail(1).set_index("code")
        )
        latest = latest.join(margin_latest[["net_financing_buy"]], how="left")

    if "net_financing_buy" not in latest.columns:
        latest["net_financing_buy"] = 0.0
    latest["net_financing_buy"] = latest["net_financing_buy"].fillna(0.0)

    # Northbound inflow (diff of holding value) - optional
    nb_inflow_map = {}
    try:
        nb_df = pd.read_sql(
            f"SELECT code, trade_date, net_inflow FROM northbound_data "
            f"WHERE code IN ({code_list_sql}) "
            f"AND trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
            conn,
        )
    except Exception:
        nb_df = pd.DataFrame()

    if not nb_df.empty:
        nb_df["trade_date"] = pd.to_datetime(nb_df["trade_date"])
        nb_df = nb_df.sort_values(["code", "trade_date"])
        nb_df["nb_inflow"] = nb_df.groupby("code")["net_inflow"].diff()
        nb_latest = nb_df.groupby("code").tail(1)
        nb_inflow_map = nb_latest.set_index("code")["nb_inflow"].fillna(0).to_dict()

    rows = []
    for code in codes:
        if code not in basic_df.index or code not in latest.index:
            continue

        info = basic_df.loc[code]
        info_dict = {
            "name": info.get("name", ""),
            "total_mv": info.get("total_mv", 0),
            "pe_ttm": info.get("pe_ttm", 0),
            "float_mv": info.get("float_mv", 0),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
        }

        close = float(latest.loc[code].get("close", 0) or 0)
        ma20 = float(latest.loc[code].get("close_20d_avg", 0) or 0)
        fin_net = float(latest.loc[code].get("net_financing_buy", 0) or 0)
        float_mv = 0.0
        try:
            float_mv = float(info_dict.get("float_mv", 0) or 0)
        except Exception:
            float_mv = 0.0

        financing_surge_pct = fin_net / float_mv if float_mv > 0 else 0.0
        nb_inflow = float(nb_inflow_map.get(code, 0.0) or 0.0)

        row_for_signal = {
            "financing_surge_pct": financing_surge_pct,
            "nb_inflow": nb_inflow,
            "net_financing_buy": fin_net,
            "close": close,
            "close_20d_avg": ma20,
        }
        signal = generate_signals_row(row_for_signal, info_dict)

        rows.append(
            {
                "Code": code,
                "Signal": signal,
                "Surge Score": round(financing_surge_pct * 1000, 2),
            }
        )

    return pd.DataFrame(rows)

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
        end_date_str = max_date_res.iloc[0]['max_date']
        if not end_date_str:
            conn.close()
            return pd.DataFrame()
        
        end_date = pd.to_datetime(end_date_str)
        start_date = end_date - pd.Timedelta(days=60)
        start_date_str = start_date.strftime('%Y-%m-%d')
    except:
        conn.close()
        return pd.DataFrame()

    print(f"Loading Market Data from {start_date_str} to {end_date_str}...")
    
    # 3. Bulk Load Tables
    daily_df = pd.read_sql(
        f"SELECT * FROM daily_market WHERE trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
        conn,
    )
    margin_df = pd.read_sql(
        f"SELECT * FROM margin_data WHERE trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
        conn,
    )
    try:
        main_df = pd.read_sql(
            f"SELECT * FROM main_fund_flow WHERE trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
            conn,
        )
    except:
        main_df = pd.DataFrame()
    try:
        nb_df = pd.read_sql(
            f"SELECT * FROM northbound_data WHERE trade_date >= '{start_date_str}' AND trade_date <= '{end_date_str}'",
            conn,
        )
    except:
        nb_df = pd.DataFrame()

    conn.close()
    
    if daily_df.empty:
        return pd.DataFrame()

    # 4. Pre-processing
    daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date'])
    if not margin_df.empty:
        margin_df['trade_date'] = pd.to_datetime(margin_df['trade_date'])
    if not main_df.empty:
        main_df['trade_date'] = pd.to_datetime(main_df['trade_date'])

    nb_hold_map = {}
    nb_inflow_map = {}
    if not nb_df.empty:
        nb_df['trade_date'] = pd.to_datetime(nb_df['trade_date'])
        nb_df = nb_df.rename(columns={'net_inflow': 'nb_hold_val'})
        nb_df = nb_df.sort_values(['code', 'trade_date'])
        nb_df['nb_inflow'] = nb_df.groupby('code')['nb_hold_val'].diff()
        nb_latest = nb_df.groupby('code').tail(1)
        nb_hold_map = nb_latest.set_index('code')['nb_hold_val'].to_dict()
        nb_inflow_map = nb_latest.set_index('code')['nb_inflow'].fillna(0).to_dict()

    # 5. Daily-only calculations (MA/Chg)
    # daily_market often updates earlier than funding tables; avoid joining by date here to prevent
    # "missing funding on latest market day -> filled as 0 -> rankings become empty" issues.
    daily_df = daily_df.sort_values(['code', 'trade_date'])
    daily_df['close_20d_avg'] = (
        daily_df.groupby('code')['close']
        .rolling(20, min_periods=1)
        .mean()
        .reset_index(0, drop=True)
    )
    daily_df['chg_pct'] = daily_df.groupby('code')['close'].pct_change() * 100

    # Latest daily snapshot per code
    latest_snapshot = daily_df.groupby('code').tail(1).copy()

    # Join latest financing/main fund snapshots by code (not by date)
    latest_snapshot = latest_snapshot.set_index('code')

    if not margin_df.empty:
        margin_latest = (
            margin_df.sort_values(['code', 'trade_date'])
            .groupby('code')
            .tail(1)
            .set_index('code')
        )
        latest_snapshot = latest_snapshot.join(
            margin_latest[
                [
                    'financing_buy',
                    'financing_balance',
                    'securities_sell',
                    'securities_balance',
                    'net_financing_buy',
                ]
            ],
            how='left',
        )

    if not main_df.empty:
        main_latest = (
            main_df.sort_values(['code', 'trade_date'])
            .groupby('code')
            .tail(1)
            .set_index('code')
        )
        latest_snapshot = latest_snapshot.join(main_latest[['main_net_inflow']], how='left')

    latest_snapshot = latest_snapshot.reset_index()

    # Fill missing fund fields
    for col in ['financing_buy', 'financing_balance', 'securities_sell', 'securities_balance', 'net_financing_buy']:
        if col not in latest_snapshot.columns:
            latest_snapshot[col] = 0.0
        latest_snapshot[col] = latest_snapshot[col].fillna(0)

    if 'main_net_inflow' not in latest_snapshot.columns:
        latest_snapshot['main_net_inflow'] = 0.0
    latest_snapshot['main_net_inflow'] = latest_snapshot['main_net_inflow'].fillna(0)

    # 6. Vectorized Calculations on snapshot
    latest_snapshot['float_mv'] = latest_snapshot['code'].map(basic_df['float_mv'])

    latest_snapshot['financing_surge_pct'] = 0.0
    mask_mv = latest_snapshot['float_mv'] > 0
    latest_snapshot.loc[mask_mv, 'financing_surge_pct'] = (
        latest_snapshot.loc[mask_mv, 'net_financing_buy'] / latest_snapshot.loc[mask_mv, 'float_mv']
    )

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

        nb_inflow_raw = nb_inflow_map.get(code, 0.0)
        nb_hold_raw = nb_hold_map.get(code, 0.0)

        # Generate Signal
        row_for_signal = row.copy()
        row_for_signal['nb_inflow'] = nb_inflow_raw
        signal = generate_signals_row(row_for_signal, info_dict)

        float_mv = 0.0
        try:
            float_mv = float(info_dict.get('float_mv', 0) or 0)
        except Exception:
            float_mv = 0.0

        total_mv = 0.0
        try:
            total_mv = float(info_dict.get('total_mv', 0) or 0)
        except Exception:
            total_mv = 0.0

        fin_net = 0.0
        try:
            fin_net = float(row.get('net_financing_buy', 0) or 0)
        except Exception:
            fin_net = 0.0

        nb_inflow = 0.0
        try:
            nb_inflow = float(nb_inflow_raw or 0)
        except Exception:
            nb_inflow = 0.0

        nb_hold_val = 0.0
        try:
            nb_hold_val = float(nb_hold_raw or 0)
        except Exception:
            nb_hold_val = 0.0

        if float_mv > 0:
            fin_mv_pct = fin_net / float_mv * 100
            nb_mv_pct = nb_inflow / float_mv * 100
        else:
            fin_mv_pct = 0.0
            nb_mv_pct = 0.0

        if total_mv > 0:
            fin_tmv_pct = fin_net / total_mv * 100
            nb_tmv_pct = nb_inflow / total_mv * 100
        else:
            fin_tmv_pct = 0.0
            nb_tmv_pct = 0.0

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
            "Northbound Hold": round(nb_hold_val / 100000000, 2), 
            "NB Inflow": round(nb_inflow / 10000, 2), 
            "Main Inflow": round(row.get('main_net_inflow', 0) / 10000, 2),
            "Fin/MV%": round(fin_mv_pct, 2),
            "NB/MV%": round(nb_mv_pct, 2),
            "Fin/TMV%": round(fin_tmv_pct, 2),
            "NB/TMV%": round(nb_tmv_pct, 2),
            "Surge Score": round(row['financing_surge_pct'] * 1000, 2) 
        })
        
    return pd.DataFrame(results)

if __name__ == '__main__':
    start_t = datetime.datetime.now()
    df = get_full_analysis_report()
    end_t = datetime.datetime.now()
    print(df.head())
    print(f"Generated report for {len(df)} stocks in {(end_t - start_t).total_seconds():.2f} seconds.")

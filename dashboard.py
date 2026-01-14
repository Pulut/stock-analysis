import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import analyzer
import sqlite3
import akshare as ak
import db
import trader 
import requests
import threading
import time
import datetime
import subprocess
import sys

# Initialize Trade System (Will create tables for user1/user2)
trader.init_trade_system()

# --- Background Scheduler ---
@st.cache_resource
def init_scheduler():
    """
    Starts a background thread to run data_loader.py daily at 01:00.
    """
    def scheduler_loop():
        print("[Scheduler] 后台调度器已启动，每天 01:00 自动更新数据...")
        while True:
            now = datetime.datetime.now()
            # Simple check: 01:00 to 01:01
            if now.hour == 1 and now.minute == 0:
                print(f"[Scheduler] 触发定时任务: {now}")
                try:
                    subprocess.run([sys.executable, "data_loader.py"], check=True)
                    print("[Scheduler] 数据更新完成！")
                except Exception as e:
                    print(f"[Scheduler] 更新失败: {e}")
                
                time.sleep(61)
            else:
                time.sleep(30)

    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    return t

init_scheduler()

# Page config
st.set_page_config(
    page_title="A股全市场资金分析系统", 
    layout="wide",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "# A股资金流向分析系统\n基于 AkShare 数据源开发。"
    }
)

# --- Custom CSS ---
hide_style = """
    <style>
    .stDeployButton {display:none;}
    footer {visibility: hidden;}
    #stDecoration {display:none;}
    </style>
"""
st.markdown(hide_style, unsafe_allow_html=True)

# --- Helper Functions ---
@st.cache_data(ttl=3600)
def load_analysis_report():
    return analyzer.get_full_analysis_report()

def enrich_with_realtime_data(df):
    if df.empty: return df
    try:
        spot_df = ak.stock_zh_a_spot_em()
        real_df = spot_df[['代码', '最新价', '涨跌幅', '今开', '昨收', '量比', '换手率']].copy()
        real_df.columns = ['Code', 'Real_Price', 'Real_Chg_Pct', 'Real_Open', 'Pre_Close', 'Vol_Ratio', 'Real_Turnover']
        
        real_df['Open_Pct'] = (real_df['Real_Open'] - real_df['Pre_Close']) / real_df['Pre_Close'] * 100
        real_df['Open_Pct'] = real_df['Open_Pct'].fillna(0).round(2)
        
        merged_df = pd.merge(df, real_df, on='Code', how='left')
        merged_df['Real_Price'] = merged_df['Real_Price'].fillna(merged_df['Close'])

        # If realtime quote is missing (e.g., suspended stock), fall back to last trading day's pct change.
        if 'Chg%' in merged_df.columns:
            merged_df['Real_Chg_Pct'] = merged_df['Real_Chg_Pct'].fillna(merged_df['Chg%'])
        merged_df['Real_Chg_Pct'] = merged_df['Real_Chg_Pct'].fillna(0)
        merged_df['Vol_Ratio'] = merged_df['Vol_Ratio'].fillna(0)
        return merged_df
    except Exception as e:
        st.warning(f"无法获取实时行情: {e}")
        return df

def load_report_df(get_realtime: bool):
    """Lazy-load the heavy analysis report only when needed by the current page."""
    with st.spinner("正在加载数据..."):
        df = load_analysis_report()

    if df is None or df.empty:
        st.error("数据未加载，请运行 data_loader.py")
        st.stop()

    # Avoid mutating Streamlit cached objects.
    df = df.copy()

    if get_realtime:
        with st.spinner("📡 同步交易所行情..."):
            df = enrich_with_realtime_data(df)
    else:
        df["Real_Price"] = df["Close"]
        df["Real_Chg_Pct"] = df.get("Chg%", 0.0)
        df["Real_Chg_Pct"] = df["Real_Chg_Pct"].fillna(0.0)
        df["Open_Pct"] = 0.0
        df["Vol_Ratio"] = 0.0

    return df

def get_stock_history(code):
    conn = analyzer.get_db_connection()
    df, info = analyzer.get_stock_data(code, conn)
    conn.close()
    return df, info

def get_db_connection():
    return analyzer.get_db_connection()

def fetch_realtime_quotes_for_codes(codes):
    """
    Fetch realtime quotes for a small list of A-share codes via Eastmoney push2 API.

    Returns: dict {code: {"price": float|None, "chg_pct": float|None, "name": str|None}}
    """
    if not codes:
        return {}

    cleaned = []
    for c in codes:
        if c is None:
            continue
        s = str(c).strip()
        if not s:
            continue
        if s.isdigit() and len(s) <= 6:
            cleaned.append(s.zfill(6))
            continue
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == 6:
            cleaned.append(digits)

    codes = sorted(set(cleaned))
    if not codes:
        return {}

    quotes = {}
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; stock_g/1.0)",
            "Accept": "application/json,text/plain,*/*",
        }
    )

    def _to_float(v, scale=None):
        try:
            if v in (None, "", "-"):
                return None
            x = float(v)
            if scale:
                x = x / scale
            return x
        except Exception:
            return None

    # Eastmoney quote endpoint (single secid per call).
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    fields = "f57,f58,f43,f170"

    for code in codes:
        # Eastmoney secid market: 1=SH, 0=SZ/BJ (covers A-shares, ETFs, etc.)
        market = "1" if code.startswith(("6", "5", "9", "11")) else "0"
        secid = f"{market}.{code}"
        try:
            resp = session.get(url, params={"secid": secid, "fields": fields}, timeout=6)
            payload = resp.json() if resp is not None else {}
        except Exception:
            continue

        if (payload or {}).get("rc") != 0:
            continue
        data = (payload or {}).get("data") or {}

        # f43/f170 are scaled by 100 on this endpoint.
        real_code = str(data.get("f57") or code).zfill(6)
        quotes[real_code] = {
            "name": data.get("f58"),
            "price": _to_float(data.get("f43"), scale=100),
            "chg_pct": _to_float(data.get("f170"), scale=100),
        }

    return quotes

# --- Northbound (Top10 Deal) helpers ---
def load_northbound_top10_deal(conn):
    """
    Load latest Northbound Top 10 deal list from DB.

    Table: northbound_top10_deal (filled by data_loader.py).
    """
    try:
        max_date = pd.read_sql("SELECT MAX(trade_date) AS max_date FROM northbound_top10_deal", conn).iloc[0, 0]
    except Exception:
        return pd.DataFrame(), None

    if not max_date:
        return pd.DataFrame(), None

    try:
        df = pd.read_sql(
            f"SELECT code, mutual_type, rank, deal_amt FROM northbound_top10_deal WHERE trade_date = '{max_date}'",
            conn,
        )
    except Exception:
        return pd.DataFrame(), max_date

    if df is None or df.empty:
        return pd.DataFrame(), max_date

    df["Code"] = df["code"].astype(str).str.zfill(6)
    df["deal_amt"] = pd.to_numeric(df["deal_amt"], errors="coerce")
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values(["deal_amt", "rank"], ascending=[False, True]).head(10)
    return df[["Code", "mutual_type", "rank", "deal_amt"]], max_date


# --- Custom Table Renderers with Buttons ---
def render_buy_list(df, unique_key, user_id):
    """Renders a detailed list of stocks with 'Buy' buttons."""
    def _fmt_colored_signed(label, value, suffix):
        try:
            value = float(value)
        except Exception:
            return "—"
        if value == 0:
            return "—"
        color = "red" if value > 0 else "green"
        return f":{color}[{label}{value:+.2f}{suffix}]"

    def _fmt_colored_pct(label, value):
        try:
            value = float(value)
        except Exception:
            return "—"
        if value == 0:
            return "—"
        color = "red" if value > 0 else "green"
        return f":{color}[{label}{value:+.2f}%]"

    def _fmt_pe_colored(pe_value):
        try:
            pe_value = float(pe_value)
        except Exception:
            return "市盈率:-"

        # Negative PE usually means亏损; keep it green (bad) to match A股 red/up green/down convention.
        if pe_value <= 0:
            return f":green[市盈率:{pe_value:.1f}]"
        if pe_value <= 20:
            return f":red[市盈率:{pe_value:.1f}]"
        if pe_value <= 50:
            return f":orange[市盈率:{pe_value:.1f}]"
        return f":violet[市盈率:{pe_value:.1f}]"

    # Code, Name, Price, Chg, Signal, Sector/MV, Ind/PE, Fin/NB/Main, Fin&NB/MV%, Surge Score, Fin&NB/TMV%, Action
    cols = st.columns([0.7, 1.0, 0.7, 0.7, 1.1, 1.3, 1.3, 1.4, 1.0, 0.8, 1.0, 0.7])
    cols[0].markdown("**代码**")
    cols[1].markdown("**名称**")
    cols[2].markdown("**现价**")
    cols[3].markdown("**涨跌**")
    cols[4].markdown("**信号建议**")
    cols[5].markdown("**板块/市值**")
    cols[6].markdown("**行业/PE**")
    cols[7].markdown("**资金(融/北/主)**")
    cols[8].markdown("**资金/流通市值%**")
    cols[9].markdown("**强度分**")
    cols[10].markdown("**资金/总市值%**")
    cols[11].markdown("**操作**")
    
    st.markdown("---")

    for idx, row in df.iterrows():
        c = st.columns([0.7, 1.0, 0.7, 0.7, 1.1, 1.3, 1.3, 1.4, 1.0, 0.8, 1.0, 0.7])
        c[0].write(row['Code'])
        c[1].write(row['Name'])
        c[2].write(f"{row.get('Real_Price', 0):.2f}")
        
        chg = row.get('Real_Chg_Pct', 0)
        color = "red" if chg > 0 else "green"
        c[3].markdown(f":{color}[{chg:.2f}%]")
        
        # Signal
        sig = row.get('Signal', '⚪️ 中性')
        c[4].write(sig)

        # New Column: Sector & Market Cap
        sec = row.get('Sector', '-')
        mv = row.get('Mkt Cap', 0)
        c[5].caption(f"{sec} | {mv}亿")
        
        # Industry & PE
        ind = row.get('Industry', '-')
        pe = row.get('PE', 0)
        pe_str = _fmt_pe_colored(pe)
        c[6].markdown(f"{ind} | {pe_str}")

        # Financing, Northbound & Main fund flow
        fin_val = row.get('Financing Net', 0)
        nb_val = row.get('NB Inflow', 0)
        main_val = row.get('Main Inflow', 0)
        fin_str = _fmt_colored_signed("融:", fin_val, "万")
        nb_str = _fmt_colored_signed("北:", nb_val, "万")
        main_str = _fmt_colored_signed("主:", main_val, "万")
        c[7].markdown(f"{fin_str} | {nb_str} | {main_str}")

        fin_pct = row.get('Fin/MV%', 0)
        nb_pct = row.get('NB/MV%', 0)
        try:
            fin_pct = float(fin_pct)
        except Exception:
            fin_pct = 0.0
        try:
            nb_pct = float(nb_pct)
        except Exception:
            nb_pct = 0.0

        fin_pct_str = _fmt_colored_pct("融:", fin_pct)
        nb_pct_str = _fmt_colored_pct("北:", nb_pct)
        c[8].markdown(f"{fin_pct_str} | {nb_pct_str}")

        score = row.get("Surge Score", 0)
        score_str = _fmt_colored_signed("", score, "")
        c[9].markdown(score_str)

        fin_tmv_pct = row.get('Fin/TMV%', 0)
        nb_tmv_pct = row.get('NB/TMV%', 0)
        try:
            fin_tmv_pct = float(fin_tmv_pct)
        except Exception:
            fin_tmv_pct = 0.0
        try:
            nb_tmv_pct = float(nb_tmv_pct)
        except Exception:
            nb_tmv_pct = 0.0

        fin_tmv_pct_str = _fmt_colored_pct("融:", fin_tmv_pct)
        nb_tmv_pct_str = _fmt_colored_pct("北:", nb_tmv_pct)
        c[10].markdown(f"{fin_tmv_pct_str} | {nb_tmv_pct_str}")
        
        # Button
        if c[11].button("🟢 买", key=f"btn_buy_{unique_key}_{user_id}_{row['Code']}"):
            price = row.get('Real_Price', 0)
            if price > 0:
                succ, msg = trader.execute_trade(user_id, 'BUY', row['Code'], row['Name'], price, 100)
                if succ: 
                    st.toast(f"✅ {msg}")
                else: 
                    st.toast(f"❌ {msg}")
            else:
                st.toast("⚠️ 无法获取价格")

def render_sell_list(df, user_id):
    """Renders holdings with 'Sell' buttons."""
    # Adjusted columns to fit buttons
    cols = st.columns([1, 1.2, 0.8, 1.4, 1, 1, 1.2, 1.3, 1.6, 1.3])
    cols[0].markdown("**代码**")
    cols[1].markdown("**名称**")
    cols[2].markdown("**持仓**")
    cols[3].markdown("**开仓时间**")
    cols[4].markdown("**成本**")
    cols[5].markdown("**现价**")
    cols[6].markdown("**市值**")
    cols[7].markdown("**盈亏(净)**")
    cols[8].markdown("**提醒**")
    cols[9].markdown("**操作**")
    
    st.markdown("---")
    
    for idx, row in df.iterrows():
        c = st.columns([1, 1.2, 0.8, 1.4, 1, 1, 1.2, 1.3, 1.6, 1.3])
        # Code - Clickable to Deep Dive using Callback to avoid State error
        def _go_to_deep_dive(target_code):
            st.session_state["deep_dive_input"] = target_code
            st.session_state["sb_nav"] = "个股深度分析"

        c[0].button(
            row['code'], 
            key=f"btn_code_{user_id}_{row['code']}",
            on_click=_go_to_deep_dive,
            kwargs={"target_code": row['code']}
        )
        c[1].write(row['name'])
        c[2].write(str(row['quantity']))

        open_time = row.get("open_time", "")
        if not open_time:
            open_time = "—"
        c[3].write(str(open_time))

        # Cost
        avg_cost = row.get("avg_cost", 0.0)
        c[4].write(f"{avg_cost:.2f}")

        # Current Price
        curr_price = row.get('current_price', 0)
        c[5].write(f"{curr_price:.2f}")

        # Market Value
        mkt_val = row.get("market_value", 0.0)
        c[6].write(f"{mkt_val:,.0f}")
        
        pnl = row.get('profit', 0)
        pnl_pct = row.get("profit_pct", 0.0)
        try:
            pnl = float(pnl or 0.0)
        except Exception:
            pnl = 0.0
        try:
            pnl_pct = float(pnl_pct or 0.0)
        except Exception:
            pnl_pct = 0.0
        color = "red" if pnl > 0 else "green"
        # P&L
        c[7].markdown(f":{color}[{pnl:,.0f} ({pnl_pct:.2f}%)]")
        
        advice = row.get("sell_advice", "—")
        if not advice:
            advice = "—"
        c[8].markdown(advice)

        # Action Buttons
        held_qty = int(row['quantity'])
        target_qty = st.session_state.get("side_qty", 100)
        sell_qty = min(target_qty, held_qty)
        
        btn_cols = c[9].columns(2)
        # Button 1: Partial Sell
        if btn_cols[0].button(f"卖", key=f"btn_sell_{user_id}_{row['code']}", help=f"卖出 {sell_qty} 股"):
            price = row.get('current_price', 0)
            if price > 0:
                succ, msg = trader.execute_trade(user_id, 'SELL', row['code'], row['name'], price, sell_qty)
                if succ: 
                    st.toast(f"✅ {msg}")
                    st.rerun() 
                else: 
                    st.toast(f"❌ {msg}")
            else:
                st.toast("⚠️ 无法获取价格")

        # Button 2: Sell All
        if btn_cols[1].button(f"清", key=f"btn_all_{user_id}_{row['code']}", help=f"一键清仓 ({held_qty} 股)"):
            price = row.get('current_price', 0)
            if price > 0:
                succ, msg = trader.execute_trade(user_id, 'SELL', row['code'], row['name'], price, held_qty)
                if succ: 
                    st.toast(f"✅ 清仓成功")
                    st.rerun() 
                else: 
                    st.toast(f"❌ {msg}")
            else:
                st.toast("⚠️ 无法获取价格")

# --- Sidebar ---
st.sidebar.title("🚀 A股资金流向分析")

# User Selection
current_user = st.sidebar.selectbox("👤 当前用户", ["user1", "user2"])
st.sidebar.caption(f"🚀 A股资金流向分析系统")

if st.sidebar.button("🔄 刷新界面/计算信号"):
    st.cache_data.clear()
    st.rerun()

get_realtime = st.sidebar.button("📡 获取实时行情 (盘中)")

if "sb_nav" not in st.session_state:
    st.session_state["sb_nav"] = "💼 我的持仓"

page = st.sidebar.radio("功能导航", ["市场概览", "个股深度分析", "💼 我的持仓"], key="sb_nav")
# --- Flash Trade Panel ---
st.sidebar.markdown("---")
st.sidebar.subheader(f"⚡ 闪电交易 ({current_user})")
with st.sidebar.container():
    trade_code = st.text_input("代码", max_chars=6, key="side_code", placeholder="600xxx")
    trade_qty = st.number_input("数量", min_value=100, step=100, value=100, key="side_qty")
    
    t_price = 0
    t_name = ""
    if len(trade_code) == 6 and trade_code.isdigit():
        conn = None
        try:
            conn = get_db_connection()
            cursor = db.get_cursor(conn)

            cursor.execute("SELECT name FROM stock_basic WHERE code=?", (trade_code,))
            res = cursor.fetchone()
            if res and res[0]:
                t_name = str(res[0])
            else:
                t_name = trade_code

            cursor.execute(
                "SELECT close FROM daily_market WHERE code=? ORDER BY trade_date DESC LIMIT 1",
                (trade_code,),
            )
            res = cursor.fetchone()
            if res and res[0] is not None:
                t_price = float(res[0])
                st.sidebar.info(f"{t_name} : {t_price:.2f} (收盘价)")
        except Exception:
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        
    c1, c2 = st.sidebar.columns(2)
    if c1.button("买入"):
        if t_price > 0:
            s, m = trader.execute_trade(current_user, 'BUY', trade_code, t_name, t_price, trade_qty)
            if s: st.toast(m); st.rerun()
            else: st.error(m)
        else: st.error("无效代码")
    if c2.button("卖出"):
        if t_price > 0:
            s, m = trader.execute_trade(current_user, 'SELL', trade_code, t_name, t_price, trade_qty)
            if s: st.toast(m); st.rerun()
            else: st.error(m)
        else: st.error("无效代码")


# --- Page 1: Market Overview ---
if page == "市场概览":
    st.title("📊 市场资金概览 (沪深全市场)")
    report_df = load_report_df(get_realtime)
    
    conn = get_db_connection()
    sentiment, up, down, last_date = analyzer.get_market_sentiment(conn)
    max_dates = analyzer.get_table_max_dates(conn)
    nb_top10_df, nb_deal_date = load_northbound_top10_deal(conn)
    conn.close()
    
    daily_date = max_dates.get("daily_market") or last_date
    margin_date = max_dates.get("margin_data") or "-"
    nb_date = max_dates.get("northbound_data") or "-"
    main_date = max_dates.get("main_fund_flow") or "-"
    nb_date = nb_deal_date or "-"

    st.markdown(
        f"**📅 分析日期(行情)**: {daily_date} | **融数据**: {margin_date} | **北向成交**: {nb_date} | **主力数据**: {main_date} "
        f"| **🌡️ 大盘**: {sentiment} (📈{up} : 📉{down})"
    )
    if nb_date != "-" and daily_date and nb_date != daily_date:
        st.caption(f"北向成交数据尚未更新到 {daily_date}，北向榜单截至 {nb_date}")
    st.progress(up/(up+down) if (up+down)>0 else 0)
    st.markdown("---")
    
    st.subheader(f"🔥 融资净买入强度榜 (Top 10, 截至 {margin_date})")
    top_financing = report_df.sort_values(by="Surge Score", ascending=False).head(10)
    render_buy_list(top_financing, "financing", current_user)
    
    st.markdown("---")
    
    st.subheader(f"💰 北向十大成交榜 (Top 10, 截至 {nb_date})")
    if nb_top10_df is None or nb_top10_df.empty:
        st.caption("暂无北向十大成交数据，请先运行 data_loader.py")
    else:
        merged = pd.merge(nb_top10_df, report_df, on="Code", how="left")
        if "Name" not in merged.columns:
            merged["Name"] = merged["Code"]
        
        # Resolve conflicting column names from merge if necessary
        # report_df has 'Name', 'Close', etc. nb_top10_df has 'name' (maybe), 'deal_amt'
        # Prioritize report_df data for display in render_buy_list
        if "Name_y" in merged.columns:
            merged["Name"] = merged["Name_y"].fillna(merged["Name_x"])
        
        merged = merged.sort_values(["deal_amt", "rank"], ascending=[False, True]).head(10)
        
        # Use render_buy_list for consistent display
        render_buy_list(merged, "north_deal", current_user)

    st.markdown("---")

    # st.subheader("⚠️ 风险预警 (资金大幅流出 Top 10)")
    # # Sort by Surge Score ascending (most negative first)
    # top_risk = report_df[report_df['Surge Score'] < 0].sort_values(by="Surge Score", ascending=True).head(10)
    # render_buy_list(top_risk, "risk", current_user)

# --- Page 2: Smart Scanner ---
# elif page == "智能选股":
#     st.title("📡 智能信号筛选器")
#     report_df = load_report_df(get_realtime)
#    
#     c1, c2, c3, c4 = st.columns(4)
#     sig = c1.multiselect("信号", report_df['Signal'].unique())
#     ind = c2.multiselect("行业", report_df['Industry'].unique())
#     sec = c3.multiselect("板块", report_df['Sector'].unique())
#     min_t = c4.slider("换手%", 0.0, 20.0, 1.0)
#    
#     filtered = report_df.copy()
#     if sig: filtered = filtered[filtered['Signal'].isin(sig)]
#     if ind: filtered = filtered[filtered['Industry'].isin(ind)]
#     if sec: filtered = filtered[filtered['Sector'].isin(sec)]
#     filtered = filtered[filtered['Turnover%'] >= min_t]
#    
#     st.caption(f"筛选结果: {len(filtered)} 只 (显示前 50 只)")
#    
#     # Render List
#     render_buy_list(filtered.head(50), "scanner", current_user)

# --- Page 3: Deep Dive ---
elif page == "个股深度分析":
    try:
        st.title("📈 个股资金透视")
        
        if "deep_dive_input" not in st.session_state:
            st.session_state["deep_dive_input"] = ""
            
        code_input = st.text_input("输入代码", key="deep_dive_input")
        if code_input:
            df, info = get_stock_history(code_input)
            if not df.empty:
                st.header(f"{info['name']} ({code_input})")
                fig = make_subplots(
                    rows=4, cols=1, 
                    shared_xaxes=True, 
                    row_heights=[0.4, 0.15, 0.25, 0.2],
                    subplot_titles=("📈 价格走势 (每日 K 线)", "📊 成交量 (手)", "💰 融资余额与净买入 (内资杠杆)", "🌊 北向持仓市值 (外资动向)")
                )
                fig.add_trace(go.Candlestick(x=df['trade_date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], name="日K"), row=1, col=1)
                fig.add_trace(go.Bar(x=df['trade_date'], y=df['volume'], name="成交量"), row=2, col=1)
                fig.add_trace(go.Scatter(x=df['trade_date'], y=df['financing_balance'], fill='tozeroy', line=dict(color='orange'), name="融资余额"), row=3, col=1)
                fig.add_trace(go.Bar(x=df['trade_date'], y=df['net_financing_buy'], marker_color='red', name="融资净买入"), row=3, col=1)
                fig.add_trace(go.Scatter(x=df['trade_date'], y=df['nb_hold_val'], line=dict(color='blue'), name="北向持仓"), row=4, col=1)
                fig.update_layout(
                    height=800, 
                    xaxis_rangeslider_visible=False, 
                    showlegend=False,
                    hovermode="x unified",
                    xaxis_tickformat="%Y-%m-%d"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("代码无效")
    except Exception as e:
        st.error(f"发生错误: {e}")
        st.exception(e)

# --- Page 4: Portfolio ---
elif page == "💼 我的持仓":
    st.title(f"💼 我的模拟持仓 ({current_user})")
    
    try:
        # Default pricing: use last close from DB for held codes (no network call).
        last_close_lookup = {}
        held_codes = []
        conn = None
        try:
            conn = get_db_connection()
            cursor = db.get_cursor(conn)
            cursor.execute("SELECT code FROM trade_positions WHERE user_id=?", (current_user,))
            held_codes = [str(r[0]).zfill(6) for r in cursor.fetchall()]
            
            if held_codes:
                # Optimized: Batch fetch last close for all held codes
                # Fetch last 30 days to cover suspensions/holidays
                placeholders = ",".join(["?"] * len(held_codes))
                # Get max date first to limit range efficiently
                cursor.execute("SELECT MAX(trade_date) FROM daily_market")
                max_date_res = cursor.fetchone()
                if max_date_res and max_date_res[0]:
                    max_d = datetime.datetime.strptime(str(max_date_res[0]), "%Y-%m-%d")
                    start_d = max_d - datetime.timedelta(days=30)
                    start_d_str = start_d.strftime("%Y-%m-%d")
                    
                    sql = f"""
                        SELECT code, close, trade_date 
                        FROM daily_market 
                        WHERE code IN ({placeholders}) 
                        AND trade_date >= ?
                        ORDER BY trade_date DESC
                    """
                    # Provide codes as params, plus start_date
                    params = held_codes + [start_d_str]
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                    
                    # Process in Python: keep first (latest) close for each code
                    seen = set()
                    for r in rows:
                        c, p, d = r[0], r[1], r[2]
                        if c not in seen and p is not None:
                            last_close_lookup[c] = float(p)
                            seen.add(c)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

        # --- Intraday controls (optional realtime quotes for holdings only) ---
        if "holdings_rt_quotes" not in st.session_state:
            st.session_state["holdings_rt_quotes"] = {}
        if "holdings_rt_ts" not in st.session_state:
            st.session_state["holdings_rt_ts"] = ""
        if "holdings_use_realtime" not in st.session_state:
            st.session_state["holdings_use_realtime"] = True

        st.caption("默认使用最新收盘价估值；盘中可刷新持仓实时价（仅持仓）用于止损/MA20 提醒。盈亏/总资产按“卖出净到手”估算（含佣金/印花税/过户费）。")
        stop_loss_pct = st.slider(
            "盘中止损阈值(%)",
            min_value=1.0,
            max_value=20.0,
            value=7.0,
            step=0.5,
            key="holdings_stop_loss_pct",
        )

        rt_cols = st.columns([1.3, 1.0, 2.2])
        refresh_clicked = rt_cols[0].button("📡 刷新实时价(仅持仓)", disabled=(len(held_codes) == 0))
        if refresh_clicked:
            with st.spinner("正在获取持仓实时价..."):
                quotes = fetch_realtime_quotes_for_codes(held_codes)
            if quotes:
                st.session_state["holdings_rt_quotes"] = quotes
                bj_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
                st.session_state["holdings_rt_ts"] = bj_now.strftime("%Y-%m-%d %H:%M:%S")
                st.session_state["holdings_use_realtime"] = True
            else:
                st.warning("实时价获取失败，仍使用收盘价。")

        use_realtime = rt_cols[1].checkbox("使用实时价", key="holdings_use_realtime", disabled=(len(held_codes) == 0))
        if st.session_state.get("holdings_rt_ts"):
            rt_cols[2].caption(f"实时价更新时间：{st.session_state['holdings_rt_ts']}")

        rt_quotes = st.session_state.get("holdings_rt_quotes") or {}
        rt_price_lookup = {
            str(code).zfill(6): (q or {}).get("price")
            for code, q in rt_quotes.items()
            if (q or {}).get("price")
        }

        price_lookup = dict(last_close_lookup)
        if use_realtime and rt_price_lookup:
            price_lookup.update(rt_price_lookup)
        elif use_realtime and held_codes and not rt_price_lookup:
            st.info("未获取到实时价，当前仍使用收盘价估值。")

        cash, total, pos = trader.get_account_info(current_user, price_lookup=price_lookup)
    except Exception as e:
        st.error("持仓加载失败（不一定是账户未初始化）。")
        with st.expander("错误详情"):
            st.exception(e)
        st.stop()
        
    pnl = total - 100000
    pnl_pct = pnl/100000*100
    
    c1, c2, c3 = st.columns(3)
    c1.metric("💰 总资产(净)", f"{total:,.0f}", f"{pnl:,.0f}", delta_color="inverse")
    c2.metric("💵 现金", f"{cash:,.0f}")
    c3.metric("📈 总收益", f"{pnl_pct:.2f}%", f"{pnl:,.0f}", delta_color="inverse")
    
    st.subheader("持仓列表")
    if not pos.empty:
        def _sell_advice_from_signal(signal):
            if not isinstance(signal, str) or not signal:
                return "—"
            if "止损离场" in signal:
                return ":red[⚠ 推荐卖出（止损离场）]"
            if "黑名单" in signal:
                return ":red[⚠ 推荐卖出（黑名单）]"
            if "资金出逃" in signal:
                return ":orange[💸 建议减仓（资金出逃）]"
            return "—"

        # Compute signals only for held codes (fast, DB-only).
        sig_df = pd.DataFrame()
        conn = None
        try:
            conn = get_db_connection()
            sig_df = analyzer.get_signals_for_codes(conn, pos["code"].tolist())
        except Exception:
            sig_df = pd.DataFrame()
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

        pos = pos.copy()
        if sig_df is not None and not sig_df.empty:
            pos = pd.merge(pos, sig_df[["Code", "Signal", "MA20"]], left_on="code", right_on="Code", how="left")
            pos.drop(columns=["Code"], inplace=True, errors="ignore")
        else:
            pos["Signal"] = ""
            pos["MA20"] = 0.0

        pos["daily_advice"] = pos["Signal"].apply(_sell_advice_from_signal)
        pos["intraday_advice"] = "—"

        # Intraday rules (only when realtime is enabled & available)
        rt_quotes_local = st.session_state.get("holdings_rt_quotes") or {}
        use_realtime_effective = bool(use_realtime and rt_quotes_local)
        if use_realtime_effective:
            pos["rt_chg_pct"] = pos["code"].astype(str).str.zfill(6).map(
                lambda c: (rt_quotes_local.get(c) or {}).get("chg_pct")
            )
            pos["rt_chg_pct"] = pd.to_numeric(pos["rt_chg_pct"], errors="coerce").fillna(0.0)

            pos["profit_pct"] = pd.to_numeric(pos.get("profit_pct"), errors="coerce").fillna(0.0)
            pos["avg_cost"] = pd.to_numeric(pos.get("avg_cost"), errors="coerce").fillna(0.0)
            pos["current_price"] = pd.to_numeric(pos.get("current_price"), errors="coerce").fillna(0.0)
            pos["MA20"] = pd.to_numeric(pos.get("MA20"), errors="coerce").fillna(0.0)

            stop_thresh = float(stop_loss_pct or 0)
            stop_mask = (pos["avg_cost"] > 0) & (pos["profit_pct"] <= -stop_thresh)
            pos.loc[stop_mask, "intraday_advice"] = pos.loc[stop_mask, "profit_pct"].apply(
                lambda v: f":red[⚠ 盘中止损 {v:.2f}%]"
            )

            ma20_mask = (
                (~stop_mask)
                & (pos["MA20"] > 0)
                & (pos["current_price"] < pos["MA20"])
                & (pos["rt_chg_pct"] < 0)
            )
            pos.loc[ma20_mask, "intraday_advice"] = ":orange[📉 盘中跌破MA20]"

        def _severity(advice):
            if isinstance(advice, str) and advice.startswith(":red["):
                return 2
            if isinstance(advice, str) and advice.startswith(":orange["):
                return 1
            return 0

        pos["sell_advice"] = pos.apply(
            lambda r: r["intraday_advice"]
            if _severity(r["intraday_advice"]) >= _severity(r["daily_advice"])
            else r["daily_advice"],
            axis=1,
        )

        # Show a concise warning list for strong sell advice.
        try:
            sell_mask = pos["sell_advice"].astype(str).str.startswith(":red[", na=False)
            sell_list = pos[sell_mask][["name", "code"]].head(10)
            if not sell_list.empty:
                items = "、".join([f"{r['name']}({r['code']})" for _, r in sell_list.iterrows()])
                st.warning(f"⚠ 推荐卖出提醒：{items}")
        except Exception:
            pass

        render_sell_list(pos, current_user)
    else:
        st.info("空仓中...")
        
    with st.expander("交易流水"):
        conn = get_db_connection()
        # Filter orders by user_id
        h = pd.read_sql(f"SELECT * FROM trade_orders WHERE user_id='{current_user}' ORDER BY id DESC LIMIT 20", conn)
        conn.close()
        if not h.empty:
            if "action" in h.columns:
                h["action"] = (
                    h["action"]
                    .astype(str)
                    .str.upper()
                    .map({"BUY": "买入", "SELL": "卖出"})
                    .fillna(h["action"])
                )

            show_cols = [
                "created_at",
                "trade_date",
                "action",
                "code",
                "name",
                "price",
                "quantity",
                "amount",
                "total_fee",
                "commission",
                "stamp_duty",
                "transfer_fee",
                "cash_change",
                "realized_pnl",
                "balance_after",
            ]
            h = h[[c for c in show_cols if c in h.columns]]
            h = h.rename(
                columns={
                    "created_at": "交易时间(北京)",
                    "trade_date": "交易日",
                    "action": "方向",
                    "code": "代码",
                    "name": "名称",
                    "price": "成交价",
                    "quantity": "数量",
                    "amount": "成交额",
                    "total_fee": "总费用",
                    "commission": "佣金",
                    "stamp_duty": "印花税",
                    "transfer_fee": "过户费",
                    "cash_change": "现金变动",
                    "realized_pnl": "本次已实现盈亏",
                    "balance_after": "余额",
                }
            )

        st.dataframe(h, use_container_width=True)

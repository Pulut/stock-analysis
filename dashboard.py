import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import analyzer
import sqlite3
import akshare as ak
import trader 
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

def get_stock_history(code):
    conn = analyzer.get_db_connection()
    df, info = analyzer.get_stock_data(code, conn)
    conn.close()
    return df, info

def get_db_connection():
    return analyzer.get_db_connection()

# --- Custom Table Renderers with Buttons ---
def render_buy_list(df, unique_key, user_id):
    """Renders a detailed list of stocks with 'Buy' buttons."""
    # Code, Name, Price, Chg, Signal, Sector/MV, Ind/PE, Fin/NB, Action
    cols = st.columns([0.7, 1.0, 0.7, 0.7, 1.1, 1.3, 1.3, 1.4, 0.7])
    cols[0].markdown("**代码**")
    cols[1].markdown("**名称**")
    cols[2].markdown("**现价**")
    cols[3].markdown("**涨跌**")
    cols[4].markdown("**信号建议**")
    cols[5].markdown("**板块/市值**")
    cols[6].markdown("**行业/PE**")
    cols[7].markdown("**资金(融/北)**")
    cols[8].markdown("**操作**")
    
    st.markdown("---")

    for idx, row in df.iterrows():
        c = st.columns([0.7, 1.0, 0.7, 0.7, 1.1, 1.3, 1.3, 1.4, 0.7])
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
        c[6].caption(f"{ind} | 市盈率:{pe}")

        # Financing & Northbound
        fin_val = row.get('Financing Net', 0)
        nb_val = row.get('NB Inflow', 0)
        fin_str = f"{fin_val}万" if fin_val != 0 else "-"
        nb_str = f"{nb_val}万" if nb_val != 0 else "-"
        c[7].caption(f"融:{fin_str} | 北:{nb_str}")
        
        # Button
        if c[8].button("🟢 买", key=f"btn_buy_{unique_key}_{user_id}_{row['Code']}"):
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
    cols = st.columns([1, 1.5, 1, 1, 1, 0.8])
    cols[0].markdown("`代码`")
    cols[1].markdown("`名称`")
    cols[2].markdown("`持仓`")
    cols[3].markdown("`现价`")
    cols[4].markdown("`盈亏`")
    cols[5].markdown("`操作`")
    
    st.markdown("---")
    
    for idx, row in df.iterrows():
        c = st.columns([1, 1.5, 1, 1, 1, 0.8])
        c[0].write(row['code'])
        c[1].write(row['name'])
        c[2].write(str(row['quantity']))
        c[3].write(f"{row.get('current_price', 0):.2f}")
        
        pnl = row.get('profit', 0)
        color = "red" if pnl > 0 else "green"
        c[4].markdown(f":{color}[{pnl:.0f}]")
        
        if c[5].button("🔴 卖", key=f"btn_sell_{user_id}_{row['code']}"):
            price = row.get('current_price', 0)
            if price > 0:
                succ, msg = trader.execute_trade(user_id, 'SELL', row['code'], row['name'], price, 100)
                if succ: 
                    st.toast(f"✅ {msg}")
                    st.rerun() 
                else: 
                    st.toast(f"❌ {msg}")
            else:
                st.toast("⚠️ 无法获取价格")

# --- Sidebar ---
st.sidebar.title("🚀 A股资金流向分析")

# User Selection
current_user = st.sidebar.selectbox("👤 当前用户", ["user1", "user2"])

if st.sidebar.button("🔄 刷新界面/计算信号"):
    st.cache_data.clear()
    st.rerun()

get_realtime = st.sidebar.button("📡 获取实时行情 (盘中)")

page = st.sidebar.radio("功能导航", ["市场概览", "智能选股", "个股深度分析", "💼 我的持仓"])
# --- Flash Trade Panel ---
st.sidebar.markdown("---")
st.sidebar.subheader(f"⚡ 闪电交易 ({current_user})")
with st.sidebar.container():
    trade_code = st.text_input("代码", max_chars=6, key="side_code", placeholder="600xxx")
    trade_qty = st.number_input("数量", min_value=100, step=100, value=100, key="side_qty")
    
    t_price = 0
    t_name = ""
    if len(trade_code) == 6:
        try:
             spot = ak.stock_zh_a_spot_em()
             r = spot[spot['代码'] == trade_code]
             if not r.empty:
                 t_price = float(r.iloc[0]['最新价'])
                 t_name = r.iloc[0]['名称']
                 st.sidebar.info(f"{t_name} : {t_price}")
        except: pass
        
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


# Load Main Data
with st.spinner("正在加载数据..."):
    report_df = load_analysis_report()

if report_df.empty:
    st.error("数据未加载，请运行 data_loader.py")
    st.stop()

if get_realtime:
    with st.spinner("📡 同步交易所行情..."):
        report_df = enrich_with_realtime_data(report_df)
else:
    report_df['Real_Price'] = report_df['Close']
    # Fallback: use last trading day's pct change from the analysis snapshot.
    report_df['Real_Chg_Pct'] = report_df.get('Chg%', 0.0)
    report_df['Real_Chg_Pct'] = report_df['Real_Chg_Pct'].fillna(0.0)
    report_df['Open_Pct'] = 0.0
    report_df['Vol_Ratio'] = 0.0

# --- Page 1: Market Overview ---
if page == "市场概览":
    st.title("📊 市场资金概览 (沪深全市场)")
    
    conn = get_db_connection()
    sentiment, up, down, last_date = analyzer.get_market_sentiment(conn)
    conn.close()
    
    st.markdown(f"**📅 分析日期**: {last_date} | **🌡️ 大盘**: {sentiment} (📈{up} : 📉{down})")
    st.progress(up/(up+down) if (up+down)>0 else 0)
    st.markdown("---")
    
    st.subheader("🔥 融资净买入强度榜 (Top 10)")
    top_financing = report_df.sort_values(by="Surge Score", ascending=False).head(10)
    render_buy_list(top_financing, "financing", current_user)
    
    st.markdown("---")
    
    st.subheader("💰 北向资金扫货榜 (Top 10)")
    top_north = report_df.sort_values(by="NB Inflow", ascending=False).head(10)
    render_buy_list(top_north, "north", current_user)

    st.markdown("---")

    st.subheader("⚠️ 风险预警 (资金大幅流出 Top 10)")
    # Sort by Surge Score ascending (most negative first)
    top_risk = report_df[report_df['Surge Score'] < 0].sort_values(by="Surge Score", ascending=True).head(10)
    render_buy_list(top_risk, "risk", current_user)

# --- Page 2: Smart Scanner ---
elif page == "智能选股":
    st.title("📡 智能信号筛选器")
    
    c1, c2, c3, c4 = st.columns(4)
    sig = c1.multiselect("信号", report_df['Signal'].unique())
    ind = c2.multiselect("行业", report_df['Industry'].unique())
    sec = c3.multiselect("板块", report_df['Sector'].unique())
    min_t = c4.slider("换手%", 0.0, 20.0, 1.0)
    
    filtered = report_df.copy()
    if sig: filtered = filtered[filtered['Signal'].isin(sig)]
    if ind: filtered = filtered[filtered['Industry'].isin(ind)]
    if sec: filtered = filtered[filtered['Sector'].isin(sec)]
    filtered = filtered[filtered['Turnover%'] >= min_t]
    
    st.caption(f"筛选结果: {len(filtered)} 只 (显示前 50 只)")
    
    # Render List
    render_buy_list(filtered.head(50), "scanner", current_user)

# --- Page 3: Deep Dive ---
elif page == "个股深度分析":
    st.title("📈 个股资金透视")
    code_input = st.text_input("输入代码", "600000")
    if code_input:
        df, info = get_stock_history(code_input)
        if not df.empty:
            st.header(f"{info['name']} ({code_input})")
            fig = make_subplots(rows=4, cols=1, shared_xaxes=True, row_heights=[0.4,0.15,0.25,0.2])
            fig.add_trace(go.Candlestick(x=df['trade_date'], open=df['open'], high=df['high'], low=df['low'], close=df['close']), row=1, col=1)
            fig.add_trace(go.Bar(x=df['trade_date'], y=df['volume']), row=2, col=1)
            fig.add_trace(go.Scatter(x=df['trade_date'], y=df['financing_balance'], fill='tozeroy', line=dict(color='orange')), row=3, col=1)
            fig.add_trace(go.Bar(x=df['trade_date'], y=df['net_financing_buy'], marker_color='red'), row=3, col=1)
            fig.add_trace(go.Scatter(x=df['trade_date'], y=df['nb_hold_val'], line=dict(color='blue')), row=4, col=1)
            fig.update_layout(height=800, xaxis_rangeslider_visible=False, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error("代码无效")

# --- Page 4: Portfolio ---
elif page == "💼 我的持仓":
    st.title(f"💼 我的模拟持仓 ({current_user})")
    
    try:
        cash, total, pos = trader.get_account_info(current_user)
    except:
        st.error("账户未初始化")
        st.stop()
        
    pnl = total - 100000
    pnl_pct = pnl/100000*100
    
    c1, c2, c3 = st.columns(3)
    c1.metric("💰 总资产", f"{total:,.0f}", f"{pnl:,.0f}")
    c2.metric("💵 现金", f"{cash:,.0f}")
    c3.metric("📈 总收益", f"{pnl_pct:.2f}%")
    
    st.subheader("持仓列表")
    if not pos.empty:
        render_sell_list(pos, current_user)
    else:
        st.info("空仓中...")
        
    with st.expander("交易流水"):
        conn = get_db_connection()
        # Filter orders by user_id
        h = pd.read_sql(f"SELECT * FROM trade_orders WHERE user_id='{current_user}' ORDER BY id DESC LIMIT 20", conn)
        conn.close()
        st.dataframe(h)

import pandas as pd
import datetime
import db

DB_PATH = db.SQLITE_DB_PATH

def get_db_connection():
    return db.get_db_connection()

def _now_beijing_str() -> str:
    # Beijing time is UTC+8 (no DST). Use UTC offset to avoid tzdata availability issues.
    bj_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    return bj_now.strftime("%Y-%m-%d %H:%M:%S")

def init_trade_system(initial_capital=100000.0, users=None, reset=False):
    """
    Initializes the trading tables for Multi-User.

    NOTE: In Streamlit, scripts rerun frequently (e.g., clicking a button). This
    function is non-destructive by default and will only create missing tables.

    Set reset=True to drop and recreate trade tables (destructive).
    """
    conn = get_db_connection()
    cursor = db.get_cursor(conn)
    backend = db.get_backend()
    
    if reset:
        # Drop old tables if exist to ensure schema update (destructive)
        cursor.execute("DROP TABLE IF EXISTS trade_account")
        cursor.execute("DROP TABLE IF EXISTS trade_positions")
        cursor.execute("DROP TABLE IF EXISTS trade_orders")
    
    # 1. Account Table (Cash) - Key: user_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_account (
            user_id TEXT PRIMARY KEY,
            cash REAL,
            total_assets REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 2. Positions Table - Key: (user_id, code)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_positions (
            user_id TEXT,
            code TEXT,
            name TEXT,
            quantity INTEGER,
            avg_cost REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, code)
        )
    ''')
    
    # 3. Order History
    order_id_ddl = "id BIGSERIAL PRIMARY KEY" if backend == "postgres" else "id INTEGER PRIMARY KEY AUTOINCREMENT"
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS trade_orders (
            {order_id_ddl},
            user_id TEXT,
            trade_date TEXT,
            code TEXT,
            name TEXT,
            action TEXT, 
            price REAL,
            quantity INTEGER,
            amount REAL,
            commission REAL,
            balance_after REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Initialize Accounts for user1 and user2 (or passed-in users)
    if users is None:
        users = ['user1', 'user2']
    for u in users:
        cursor.execute(
            """
            INSERT INTO trade_account (user_id, cash, total_assets)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (u, initial_capital, initial_capital),
        )
        
    conn.commit()
    conn.close()
    print(f"多用户交易系统初始化完成！用户: {users}, 初始资金: {initial_capital}, reset={reset}")

def get_account_info(user_id, price_lookup=None):
    """Returns cash, total assets, and positions for a SPECIFIC user.

    price_lookup: optional dict mapping code(str)->price(float) so we can value
    positions without making network calls.
    """
    conn = get_db_connection()
    try:
        cursor = db.get_cursor(conn)
        # Get Cash
        cursor.execute("SELECT cash FROM trade_account WHERE user_id=?", (user_id,))
        res = cursor.fetchone()
        if not res:
            return 0, 0, pd.DataFrame()
        cash = float(res[0] or 0.0)
        
        # Get Positions
        positions = pd.read_sql(f"SELECT * FROM trade_positions WHERE user_id='{user_id}'", conn)
        
        market_val = 0.0
        
        if not positions.empty:
            positions["code"] = positions["code"].astype(str).str.zfill(6)
            qty = pd.to_numeric(positions.get("quantity"), errors="coerce").fillna(0.0)
            avg_cost = pd.to_numeric(positions.get("avg_cost"), errors="coerce").fillna(0.0)

            if price_lookup:
                prices = positions["code"].map(lambda c: price_lookup.get(c))
                prices = pd.to_numeric(prices, errors="coerce").fillna(avg_cost)
            else:
                prices = avg_cost.copy()

            positions["current_price"] = prices
            positions["market_value"] = prices * qty
            positions["profit"] = (prices - avg_cost) * qty
            positions["profit_pct"] = 0.0
            mask = avg_cost > 0
            positions.loc[mask, "profit_pct"] = (prices[mask] - avg_cost[mask]) / avg_cost[mask] * 100.0

            market_val = float(positions["market_value"].sum() or 0.0)
        
        total_assets = cash + market_val
        
        # Update DB
        cursor.execute("UPDATE trade_account SET total_assets = ? WHERE user_id=?", (total_assets, user_id))
        conn.commit()
        
        return cash, total_assets, positions
    finally:
        conn.close()

def execute_trade(user_id, action, code, name, price, quantity):
    """
    Executes a trade for a SPECIFIC user.
    """
    conn = get_db_connection()
    cursor = db.get_cursor(conn)
    
    try:
        # Get current cash
        cursor.execute("SELECT cash FROM trade_account WHERE user_id=?", (user_id,))
        res = cursor.fetchone()
        if not res: return False, "用户不存在"
        current_cash = res[0]
        
        amount = price * quantity
        commission = max(5, amount * 0.0003)
        
        if action == 'BUY':
            cost = amount + commission
            if current_cash < cost:
                return False, "资金不足"
            
            new_cash = current_cash - cost
            
            # Update Position
            cursor.execute("SELECT quantity, avg_cost FROM trade_positions WHERE user_id=? AND code=?", (user_id, code))
            res = cursor.fetchone()
            if res:
                old_q, old_cost = res
                new_q = old_q + quantity
                new_cost = ((old_q * old_cost) + cost) / new_q
                cursor.execute("UPDATE trade_positions SET quantity=?, avg_cost=? WHERE user_id=? AND code=?", 
                               (new_q, new_cost, user_id, code))
            else:
                cursor.execute("INSERT INTO trade_positions (user_id, code, name, quantity, avg_cost) VALUES (?, ?, ?, ?, ?)", 
                               (user_id, code, name, quantity, price))
            
        elif action == 'SELL':
            income = amount - commission
            
            cursor.execute("SELECT quantity, avg_cost FROM trade_positions WHERE user_id=? AND code=?", (user_id, code))
            res = cursor.fetchone()
            if not res or res[0] < quantity:
                return False, "持仓不足"
            
            old_q, old_cost = res
            new_q = old_q - quantity
            new_cash = current_cash + income
            
            if new_q == 0:
                cursor.execute("DELETE FROM trade_positions WHERE user_id=? AND code=?", (user_id, code))
            else:
                cursor.execute("UPDATE trade_positions SET quantity=? WHERE user_id=? AND code=?", (new_q, user_id, code))
        
        # Update Account
        cursor.execute("UPDATE trade_account SET cash=? WHERE user_id=?", (new_cash, user_id))
        
        # Log Order
        date_str = _now_beijing_str()
        cursor.execute('''
            INSERT INTO trade_orders (user_id, trade_date, code, name, action, price, quantity, amount, commission, balance_after, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, date_str, code, name, action, price, quantity, amount, commission, new_cash, date_str))
        
        conn.commit()
        return True, f"交易成功! {action} {quantity}股"
        
    except Exception as e:
        return False, f"交易失败: {e}"
    finally:
        conn.close()

if __name__ == "__main__":
    init_trade_system()

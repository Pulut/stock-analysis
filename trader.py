import sqlite3
import pandas as pd
import datetime
import akshare as ak

DB_PATH = "stock_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    # Enable WAL mode for better concurrency (Writer doesn't block Readers)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_trade_system(initial_capital=100000.0):
    """
    Initializes the trading tables for Multi-User.
    WARNING: Drops existing trade tables to apply schema changes.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop old tables if exist to ensure schema update
    cursor.execute("DROP TABLE IF EXISTS trade_account")
    cursor.execute("DROP TABLE IF EXISTS trade_positions")
    cursor.execute("DROP TABLE IF EXISTS trade_orders")
    
    # 1. Account Table (Cash) - Key: user_id
    cursor.execute('''
        CREATE TABLE trade_account (
            user_id TEXT PRIMARY KEY,
            cash REAL,
            total_assets REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 2. Positions Table - Key: (user_id, code)
    cursor.execute('''
        CREATE TABLE trade_positions (
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
    cursor.execute('''
        CREATE TABLE trade_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    
    # Initialize Accounts for user1 and user2
    users = ['user1', 'user2']
    for u in users:
        cursor.execute("INSERT OR IGNORE INTO trade_account (user_id, cash, total_assets) VALUES (?, ?, ?)", 
                       (u, initial_capital, initial_capital))
        
    conn.commit()
    conn.close()
    print(f"多用户交易系统初始化完成！用户: {users}, 初始资金: {initial_capital}")

def get_account_info(user_id):
    """Returns cash, total assets, and positions for a SPECIFIC user."""
    conn = get_db_connection()
    try:
        # Get Cash
        res = conn.execute("SELECT cash FROM trade_account WHERE user_id=?", (user_id,)).fetchone()
        if not res: return 0, 0, pd.DataFrame()
        cash = res[0]
        
        # Get Positions
        positions = pd.read_sql(f"SELECT * FROM trade_positions WHERE user_id='{user_id}'", conn)
        
        market_val = 0.0
        
        if not positions.empty:
            # Fetch prices
            try:
                df = ak.stock_zh_a_spot_em()
                for idx, row in positions.iterrows():
                    match = df[df['代码'] == row['code']]
                    price = float(match.iloc[0]['最新价']) if not match.empty else row['avg_cost']
                    
                    positions.at[idx, 'current_price'] = price
                    positions.at[idx, 'market_value'] = price * row['quantity']
                    positions.at[idx, 'profit'] = (price - row['avg_cost']) * row['quantity']
                    positions.at[idx, 'profit_pct'] = (price - row['avg_cost']) / row['avg_cost'] * 100
                    
                    market_val += price * row['quantity']
            except:
                pass
        
        total_assets = cash + market_val
        
        # Update DB
        conn.execute("UPDATE trade_account SET total_assets = ? WHERE user_id=?", (total_assets, user_id))
        conn.commit()
        
        return cash, total_assets, positions
    finally:
        conn.close()

def execute_trade(user_id, action, code, name, price, quantity):
    """
    Executes a trade for a SPECIFIC user.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
        date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            INSERT INTO trade_orders (user_id, trade_date, code, name, action, price, quantity, amount, commission, balance_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, date_str, code, name, action, price, quantity, amount, commission, new_cash))
        
        conn.commit()
        return True, f"交易成功! {action} {quantity}股"
        
    except Exception as e:
        return False, f"交易失败: {e}"
    finally:
        conn.close()

if __name__ == "__main__":
    init_trade_system()


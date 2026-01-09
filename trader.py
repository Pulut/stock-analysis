import pandas as pd
import datetime
import os
import db

DB_PATH = db.SQLITE_DB_PATH

def get_db_connection():
    return db.get_db_connection()

def _now_beijing_str() -> str:
    # Beijing time is UTC+8 (no DST). Use UTC offset to avoid tzdata availability issues.
    bj_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    return bj_now.strftime("%Y-%m-%d %H:%M:%S")

COMMISSION_RATE = float(os.environ.get("TRADE_COMMISSION_RATE", "0.0003"))  # 0.03%
MIN_COMMISSION = float(os.environ.get("TRADE_MIN_COMMISSION", "5"))  # RMB
STAMP_DUTY_RATE = float(os.environ.get("TRADE_STAMP_DUTY_RATE", "0.001"))  # 0.1% (SELL stocks only)
TRANSFER_FEE_RATE_SH = float(os.environ.get("TRADE_TRANSFER_FEE_RATE_SH", "0.00001"))  # 0.001% (SH only)


def _round_money(v: float) -> float:
    try:
        return round(float(v), 2)
    except Exception:
        return 0.0


def _normalize_code(code: str) -> str:
    if code is None:
        return ""
    s = str(code).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    if len(digits) <= 6:
        return digits.zfill(6)
    return digits[-6:]


def _market_for_code(code: str) -> str:
    # Eastmoney-compatible: 1=SH, 0=SZ/BJ. Here we only need SH vs non-SH for fees.
    code = _normalize_code(code)
    if not code:
        return "SZ"
    return "SH" if code.startswith(("6", "5", "9", "11")) else "SZ"


def _instrument_type(code: str, name: str = "") -> str:
    """
    Rough instrument classification for fee rules.
    - stock: default
    - fund: ETFs/LOF etc (no stamp duty)
    - bond: bonds/convertible bonds (simplified)
    """
    code = _normalize_code(code)
    name = str(name or "")

    if name:
        upper = name.upper()
        if "ETF" in upper or "LOF" in upper:
            return "fund"
        if "债" in name or "转债" in name:
            return "bond"
        if "基金" in name:
            return "fund"

    if code.startswith(("11", "12", "13")):
        return "bond"
    if code.startswith(("15", "16", "18", "5")):
        return "fund"
    return "stock"


def calc_trade_fees(action: str, code: str, name: str, amount: float) -> dict:
    """
    Returns fee breakdown for one trade (approximate A-share rules).
    """
    action = str(action or "").upper().strip()
    code = _normalize_code(code)
    amount = max(0.0, float(amount or 0.0))

    instrument = _instrument_type(code, name)
    market = _market_for_code(code)

    commission = 0.0
    if amount > 0:
        commission = max(MIN_COMMISSION, amount * COMMISSION_RATE)
    commission = _round_money(commission)

    stamp_duty = 0.0
    if action == "SELL" and instrument == "stock" and amount > 0:
        stamp_duty = _round_money(amount * STAMP_DUTY_RATE)

    transfer_fee = 0.0
    if market == "SH" and instrument != "bond" and amount > 0:
        transfer_fee = _round_money(amount * TRANSFER_FEE_RATE_SH)

    total_fee = _round_money(commission + stamp_duty + transfer_fee)
    return {
        "commission": commission,
        "stamp_duty": stamp_duty,
        "transfer_fee": transfer_fee,
        "total_fee": total_fee,
        "instrument": instrument,
        "market": market,
    }


def _get_table_columns(cursor, backend: str, table: str) -> set:
    try:
        if backend == "sqlite":
            cursor.execute(f"PRAGMA table_info({table})")
            return {str(r[1]) for r in cursor.fetchall() if r and len(r) > 1}
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=?
            """,
            (table,),
        )
        return {str(r[0]) for r in cursor.fetchall() if r}
    except Exception:
        return set()


def _ensure_trade_schema(cursor, backend: str):
    """
    Add missing columns for existing tables (non-destructive migrations).
    """
    acct_cols = _get_table_columns(cursor, backend, "trade_account")
    if "cash" not in acct_cols:
        if backend == "postgres":
            cursor.execute("ALTER TABLE trade_account ADD COLUMN IF NOT EXISTS cash REAL")
        else:
            cursor.execute("ALTER TABLE trade_account ADD COLUMN cash REAL")
    if "total_assets" not in acct_cols:
        if backend == "postgres":
            cursor.execute("ALTER TABLE trade_account ADD COLUMN IF NOT EXISTS total_assets REAL")
        else:
            cursor.execute("ALTER TABLE trade_account ADD COLUMN total_assets REAL")
    if "updated_at" not in acct_cols:
        if backend == "postgres":
            cursor.execute(
                "ALTER TABLE trade_account ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )
        else:
            cursor.execute(
                "ALTER TABLE trade_account ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )

    pos_cols = _get_table_columns(cursor, backend, "trade_positions")
    if "open_time" not in pos_cols:
        if backend == "postgres":
            cursor.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS open_time TEXT")
        else:
            cursor.execute("ALTER TABLE trade_positions ADD COLUMN open_time TEXT")
    if "last_trade_time" not in pos_cols:
        if backend == "postgres":
            cursor.execute(
                "ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS last_trade_time TEXT"
            )
        else:
            cursor.execute("ALTER TABLE trade_positions ADD COLUMN last_trade_time TEXT")

    order_cols = _get_table_columns(cursor, backend, "trade_orders")
    for col, col_type in [
        ("stamp_duty", "REAL"),
        ("transfer_fee", "REAL"),
        ("total_fee", "REAL"),
        ("cash_change", "REAL"),
        ("realized_pnl", "REAL"),
    ]:
        if col in order_cols:
            continue
        if backend == "postgres":
            cursor.execute(f"ALTER TABLE trade_orders ADD COLUMN IF NOT EXISTS {col} {col_type}")
        else:
            cursor.execute(f"ALTER TABLE trade_orders ADD COLUMN {col} {col_type}")

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
            open_time TEXT,
            last_trade_time TEXT,
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
            stamp_duty REAL,
            transfer_fee REAL,
            total_fee REAL,
            cash_change REAL,
            realized_pnl REAL,
            balance_after REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Non-destructive migrations for existing DBs.
    _ensure_trade_schema(cursor, backend)
    
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
        liquidation_val = 0.0
        
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

            # Estimate liquidation value & profit net of SELL fees (more realistic).
            est_sell_fee = []
            for _, row in positions.iterrows():
                amount = float(row.get("market_value") or 0.0)
                c = str(row.get("code") or "")
                n = str(row.get("name") or "")
                fee = calc_trade_fees("SELL", c, n, amount).get("total_fee", 0.0)
                est_sell_fee.append(float(fee or 0.0))
            positions["est_sell_fee"] = pd.to_numeric(est_sell_fee, errors="coerce").fillna(0.0)
            positions["liquidation_value"] = positions["market_value"] - positions["est_sell_fee"]

            base = avg_cost * qty
            positions["profit"] = positions["liquidation_value"] - base
            positions["profit_pct"] = 0.0
            mask = base > 0
            positions.loc[mask, "profit_pct"] = positions.loc[mask, "profit"] / base[mask] * 100.0

            market_val = float(positions["market_value"].sum() or 0.0)
            liquidation_val = float(positions["liquidation_value"].sum() or 0.0)

            # Backfill open_time / last_trade_time for old rows (best effort).
            if "open_time" in positions.columns:
                try:
                    missing = positions["open_time"].isna() | (
                        positions["open_time"].astype(str).str.strip() == ""
                    )
                    for idx in positions[missing].index.tolist():
                        c = str(positions.loc[idx, "code"] or "").zfill(6)
                        cursor.execute(
                            "SELECT MIN(created_at) FROM trade_orders WHERE user_id=? AND code=? AND action='BUY'",
                            (user_id, c),
                        )
                        r = cursor.fetchone()
                        if r and r[0]:
                            t = str(r[0])
                            positions.loc[idx, "open_time"] = t
                            cursor.execute(
                                "UPDATE trade_positions SET open_time=? WHERE user_id=? AND code=?",
                                (t, user_id, c),
                            )
                except Exception:
                    pass

            if "last_trade_time" in positions.columns:
                try:
                    missing = positions["last_trade_time"].isna() | (
                        positions["last_trade_time"].astype(str).str.strip() == ""
                    )
                    for idx in positions[missing].index.tolist():
                        c = str(positions.loc[idx, "code"] or "").zfill(6)
                        cursor.execute(
                            "SELECT MAX(created_at) FROM trade_orders WHERE user_id=? AND code=?",
                            (user_id, c),
                        )
                        r = cursor.fetchone()
                        if r and r[0]:
                            t = str(r[0])
                            positions.loc[idx, "last_trade_time"] = t
                            cursor.execute(
                                "UPDATE trade_positions SET last_trade_time=? WHERE user_id=? AND code=?",
                                (t, user_id, c),
                            )
                except Exception:
                    pass
        
        # Total assets using liquidation value (net of estimated SELL fees).
        total_assets = cash + (liquidation_val if not positions.empty else 0.0)
        
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
        action = str(action or "").upper().strip()
        code = _normalize_code(code)
        name = str(name or "")
        price = float(price or 0.0)
        quantity = int(quantity or 0)

        if action not in ("BUY", "SELL"):
            return False, "非法操作"
        if not code:
            return False, "代码无效"
        if quantity <= 0:
            return False, "数量必须>0"
        if price <= 0:
            return False, "价格无效"

        # Get current cash
        cursor.execute("SELECT cash FROM trade_account WHERE user_id=?", (user_id,))
        res = cursor.fetchone()
        if not res:
            return False, "账户未初始化"
        current_cash = float(res[0] or 0.0)

        trade_time = _now_beijing_str()
        trade_date = trade_time[:10]

        amount = _round_money(price * quantity)
        fees = calc_trade_fees(action, code, name, amount)
        commission = float(fees.get("commission") or 0.0)
        stamp_duty = float(fees.get("stamp_duty") or 0.0)
        transfer_fee = float(fees.get("transfer_fee") or 0.0)
        total_fee = float(fees.get("total_fee") or 0.0)

        cash_change = 0.0
        realized_pnl = None

        if action == "BUY":
            cost = _round_money(amount + total_fee)
            if current_cash < cost:
                return False, "资金不足"

            cash_change = -cost
            new_cash = _round_money(current_cash + cash_change)

            cursor.execute(
                "SELECT quantity, avg_cost, open_time FROM trade_positions WHERE user_id=? AND code=?",
                (user_id, code),
            )
            r = cursor.fetchone()
            if r:
                old_q = int(r[0] or 0)
                old_avg = float(r[1] or 0.0)
                open_time = str(r[2] or "").strip() if len(r) > 2 else ""
                new_q = old_q + quantity
                new_total_cost = (old_q * old_avg) + cost
                new_avg = round(new_total_cost / new_q, 4)
                if not open_time:
                    open_time = trade_time
                cursor.execute(
                    """
                    UPDATE trade_positions
                    SET name=?, quantity=?, avg_cost=?, open_time=?, last_trade_time=?, updated_at=?
                    WHERE user_id=? AND code=?
                    """,
                    (name, new_q, new_avg, open_time, trade_time, trade_time, user_id, code),
                )
            else:
                avg_cost = round(cost / quantity, 4)
                cursor.execute(
                    """
                    INSERT INTO trade_positions (user_id, code, name, quantity, avg_cost, open_time, last_trade_time, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, code, name, quantity, avg_cost, trade_time, trade_time, trade_time),
                )

        else:  # SELL
            cursor.execute(
                "SELECT quantity, avg_cost FROM trade_positions WHERE user_id=? AND code=?",
                (user_id, code),
            )
            r = cursor.fetchone()
            if not r or int(r[0] or 0) < quantity:
                return False, "持仓不足"

            old_q = int(r[0] or 0)
            old_avg = float(r[1] or 0.0)

            income = _round_money(amount - total_fee)
            cash_change = income
            new_cash = _round_money(current_cash + income)
            realized_pnl = _round_money(income - (old_avg * quantity))

            new_q = old_q - quantity
            if new_q <= 0:
                cursor.execute("DELETE FROM trade_positions WHERE user_id=? AND code=?", (user_id, code))
            else:
                cursor.execute(
                    """
                    UPDATE trade_positions
                    SET quantity=?, last_trade_time=?, updated_at=?
                    WHERE user_id=? AND code=?
                    """,
                    (new_q, trade_time, trade_time, user_id, code),
                )

        # Update Account
        cursor.execute(
            "UPDATE trade_account SET cash=?, updated_at=? WHERE user_id=?",
            (new_cash, trade_time, user_id),
        )

        # Log Order
        cursor.execute(
            """
            INSERT INTO trade_orders (
                user_id, trade_date, code, name, action, price, quantity, amount,
                commission, stamp_duty, transfer_fee, total_fee, cash_change, realized_pnl,
                balance_after, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                trade_date,
                code,
                name,
                action,
                price,
                quantity,
                amount,
                commission,
                stamp_duty,
                transfer_fee,
                total_fee,
                cash_change,
                realized_pnl,
                new_cash,
                trade_time,
            ),
        )

        conn.commit()
        fee_msg = f"费用{total_fee:.2f}(佣金{commission:.2f},印花{stamp_duty:.2f},过户{transfer_fee:.2f})"
        return True, f"交易成功! {action} {quantity}股, {fee_msg}"
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"交易失败: {e}"
    finally:
        conn.close()

if __name__ == "__main__":
    init_trade_system()

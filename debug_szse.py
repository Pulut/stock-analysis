import akshare as ak
import pandas as pd
import datetime

def test_szse_margin():
    code = "000001"
    start_date = "20250101"
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    
    print(f"Fetching SZSE margin data for {code}...")
    try:
        margin_df = ak.stock_margin_detail_szse(symbol=code, start_date=start_date, end_date=end_date)
        if margin_df.empty:
            print("❌ Returned DataFrame is EMPTY!")
        else:
            print("✅ Data fetched successfully.")
            print("Columns found:", margin_df.columns.tolist())
            print("First row:", margin_df.iloc[0].to_dict())
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_szse_margin()

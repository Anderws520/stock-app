import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 3.0", layout="wide")
st.title("🛡️ 買點定位系統 (自動補齊版)")

def load_db():
    """載入資料庫並驗證結構完整性"""
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            if not df.empty and '日期' in df.columns:
                return df
        except:
            pass
    return pd.DataFrame()

def save_db(df):
    """強制字串化存檔，解決格式報錯"""
    try:
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except:
        return False

# --- 核心同步邏輯 ---
db = load_db()

# 設定起始日：若無資料則從 2026-04-27 開始
if db.empty:
    start_dt = datetime(2026, 4, 27)
else:
    last_date_str = db['日期'].max()
    start_dt = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)

today_dt = datetime.now()

if start_dt <= today_dt:
    with st.status("🔍 正在檢查並補齊數據...", expanded=True) as status:
        curr = start_dt
        while curr <= today_dt:
            d_str = curr.strftime("%Y-%m-%d")
            # 跳過週末與 5/1 勞動節
            if curr.weekday() >= 5 or d_str == "2026-05-01":
                st.write(f"🏮 {d_str} 休市，自動跳過")
            else:
                try:
                    time.sleep(3) # 穩定請求間隔
                    r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=25)
                    data = r.json()
                    
                    if data.get('stat') == 'OK' and data.get('data'):
                        new_df = pd.DataFrame(data['data'], columns=data['fields'])
                        new_df.columns = [c.strip() for c in new_df.columns]
                        new_df['日期'] = d_str
                        # 合併並存檔
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        save_db(db)
                        st.write(f"✅ {d_str} 數據同步成功")
                    else:
                        st.write(f"ℹ️ {d_str} 無開盤數據回傳")
                except Exception as e:
                    st.write(f"⏳ {d_str} 連線繁忙，下次啟動再補")
                    break # 遇到網路問題先停止，避免無限卡住
            curr += timedelta(days=1)
        status.update(label="數據檢查完畢", state="complete")

# --- 報表顯示 (4/27 以後的最新資料) ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    
    # 動態識別欄位
    buy_col = next((c for c in db.columns if '買賣超' in c or '合計' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    if buy_col and price_col:
        res = []
        for _, row in df_now.iterrows():
            try:
                shares = float(str(row[buy_col]).replace(',', ''))
                if shares <= 0: continue
                
                # 計算 5 日均價 (備援邏輯)
                hist = db[db['證券代號'] == row['證券代號']].sort_values('日期', ascending=False).head(5)

import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 3.1", layout="wide")
st.title("🛡️ 買點定位系統 (4/27 起始穩定版)")

def load_db():
    """載入並驗證資料庫"""
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            if not df.empty and '日期' in df.columns:
                return df
            os.remove(DB_FILE)
        except:
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
    return pd.DataFrame()

def save_db(df):
    """字串化存檔，防止類型錯誤"""
    try:
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except:
        return False

# --- 核心同步邏輯 ---
db = load_db()

# 根據您的要求：若無資料則從 4/27 開始
if db.empty:
    start_dt = datetime(2026, 4, 27)
else:
    last_date_str = db['日期'].max()
    start_dt = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)

today_dt = datetime.now()

if start_dt <= today_dt:
    with st.status("🔍 數據同步檢查中...", expanded=True) as status:
        curr = start_dt
        while curr <= today_dt:
            d_str = curr.strftime("%Y-%m-%d")
            # 跳過週末與 5/1 勞動節
            if curr.weekday() >= 5 or d_str == "2026-05-01":
                st.write(f"🏮 {d_str} 休市跳過")
            else:
                try:
                    time.sleep(3) # 穩定請求間隔
                    r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=25)
                    data = r.json()
                    
                    if data.get('stat') == 'OK' and data.get('data'):
                        new_df = pd.DataFrame(data['data'], columns=data['fields'])
                        new_df.columns = [c.strip() for c in new_df.columns]
                        new_df['日期'] = d_str
                        
                        # 確保具備關鍵欄位再合併
                        if any('收盤價' in c for c in new_df.columns):
                            db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                            save_db(db)
                            st.write(f"✅ {d_str} 同步完成")
                    else:
                        st.write(f"ℹ️ {d_str} 無開盤數據")
                except Exception as e:
                    st.write(f"⏳ {d_str} 連線繁忙，稍後重試")
                    break 
            curr += timedelta(days=1)
        status.update(label="數據補完結束", state="complete")

# --- 報表顯示 ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    
    # 智慧欄位尋找
    buy_col = next((c for c in db.columns if '買賣超' in c or '合計' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    if buy_col and price_col:
        res = []
        for _, row in df_now.iterrows():
            try:
                # 計算單一股票
                shares = float(str(row[buy_col]).replace(',', ''))
                if shares <= 0: continue
                
                # 計算 5 日均價 (修復 SyntaxError 的部分)
                hist_data = db[db['證券代號'] == row['證券代號']].sort_values('日期', ascending=False).head(5)
                prices = [float(str(p).replace(',', '')) for p in hist_data[price_col] if str(p).replace('.', '').isdigit()]
                ma5 = round(sum(prices)/len(prices), 2) if prices else 0
                
                res.append({
                    '代號': row['證券代號'], '名稱': row['證券名稱'],
                    '買超張數': int(round(shares/1000, 0)), '5日均價': ma5,
                    '現價': row[price_col], '最後同步': latest
                })
            except:
                continue # 若單一股票資料異常則跳過
        
        st.subheader(f"📊 分析報表：{latest}")
        st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 資料庫欄位不全，請等待 4/27 數據補齊")
else:
    st.info("💡 系統正從 4/27 開始建立資料庫，請稍候並保持網頁開啟。")

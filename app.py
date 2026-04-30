import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import re

# --- 核心設定 ---
DB_FILE = "stock_database.parquet" # 使用高效能 Parquet 格式保留昨日數據
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.0", layout="wide")
st.title("🛡️ 買點定位系統 (自動增量更新版)")

def to_num(v):
    try:
        return float(str(v).replace(',', '').replace(' ', '').strip())
    except: return 0.0

@st.cache_data(ttl=3600)
def get_live_price(sid):
    """抓取現價，增加快取機制避免過度請求"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://www.google.com/finance/quote/{sid}:TPE"
        r = requests.get(url, headers=headers, timeout=3)
        m = re.search(r'data-last-price="([\d\.]+)"', r.text)
        if m: return float(m.group(1))
    except: pass
    return None

def load_database():
    """載入已儲存的歷史資料"""
    if os.path.exists(DB_FILE):
        return pd.read_parquet(DB_FILE)
    return pd.DataFrame()

def save_database(df):
    """儲存資料庫到硬碟"""
    df.to_parquet(DB_FILE, index=False)

# --- 增量更新邏輯 ---
db = load_database()
last_date_in_db = db['日期'].max() if not db.empty else "2026-01-01"
today_str = datetime.now().strftime("%Y-%m-%d")

st.info(f"💾 資料庫最後更新日：{last_date_in_db}")

# 1. 偵測缺失日期並自動補齊
if last_date_in_db < today_str:
    with st.status("正在自動同步缺失數據...", expanded=True) as status:
        missing_dates = pd.date_range(
            start=datetime.strptime(last_date_in_db, "%Y-%m-%d") + timedelta(days=1),
            end=datetime.now()
        ).strftime("%Y-%m-%d").tolist()
        
        new_records = []
        for d in missing_dates:
            st.write(f"🔍 檢查 {d} 證交所數據...")
            try:
                r = requests.get(f"{GAS_URL}?date={d.replace('-', '')}", timeout=10)
                json_data = r.json()
                if json_data.get('stat') == 'OK':
                    df_day = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                    df_day.columns = [c.strip() for c in df_day.columns]
                    df_day['日期'] = d
                    new_records.append(df_day)
                    st.write(f"✅ {d} 同步成功")
            except: pass
        
        if new_records:
            updated_db = pd.concat([db] + new_records, ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
            save_database(updated_db)
            db = updated_db
        status.update(label="數據同步完成！", state="complete")

# --- 全樣本計算 (12 欄位) ---
if not db.empty:
    # 鎖定最新交易日
    target_date = db['日期'].max()
    latest_data = db[db['日期'] == target_date]
    
    # 動態欄位識別 (解決圖 的 KeyError)
    buy_col = next((c for c in db.columns if '三大法人買賣超股數' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)

    results = []
    # 僅針對有交易的標的進行全樣本比較
    for _, row in latest_data.iterrows():
        sid = row['證券代號']
        
        # 1. 買超張數 (股轉張)
        vol = round(to_num(row[buy_col]) / 1000, 0) if buy_col else 0
        
        # 2. 歷史 5 日均價 (直接從資料庫抓，不用重爬)
        hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False).head(5)
        ma5 = round(hist[price_col].apply(to_num).mean(), 2) if price_col else 0
        
        # 3. 現價與價差
        curr_p = get_live_price(sid) or to_num(row[price_col])
        diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
        
        # 4. 出現天數 (計算法人連續買超天數)
        days = len(db[(db['證券代號'] == sid) & (db[buy_col].apply(to_num) > 0)])

        # 完全對齊圖 的 12 個欄位
        results.append({
            '日期': target_date,
            '股票代號': sid,
            '股票名稱': row['證券名稱'],
            '關鍵分點': "三大法人",
            '買超張數': int(vol),
            '5日均價': ma5,
            '目前現價': curr_p,
            '價差%': f"{diff_pct:.2%}",
            '連續出現天數': days,
            '集保人數變動': "無數據",
            '最佳購買日期': target_date if -0.01 <= diff_pct <= 0.01 else "觀望",
            '操盤建議': "雙強初現" if vol > 500 and days <= 2 else "趨勢續強"
        })

    final_df = pd.DataFrame(results)
    final_cols = ['日期', '股票代號', '股票名稱', '關鍵分點', '買超張數', '5日均價', '目前現價', '價差%', '連續出現天數', '集保人數變動', '最佳購買日期', '操盤建議']
    
    st.dataframe(
        final_df[final_cols].sort_values('買超張數', ascending=False),
        use_container_width=True, 
        hide_index=True
    )
else:
    st.warning("資料庫初始化中，請稍候...")

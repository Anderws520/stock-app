import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import re

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.3", layout="wide")
st.title("🛡️ 買點定位系統 (最終穩定版)")

def to_num(v):
    try:
        if pd.isna(v): return 0.0
        return float(str(v).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def is_market_closed(dt):
    """手動定義 2026 台灣重要休假日，避免依賴外部套件"""
    # 週末固定休市
    if dt.weekday() >= 5: return True
    
    d_str = dt.strftime("%m%d")
    # 2026 國定假日清單 (範例：元旦、春節期間、清明、勞動、端午)
    holidays_2026 = [
        "0101", "0119", "0120", "0121", "0122", "0123", "0126", # 春節
        "0227", "0228", "0403", "0406", "0501", "0619"
    ]
    return d_str in holidays_2026

def save_db(df):
    """格式統一化儲存，解決 ArrowTypeError"""
    try:
        df = df.astype(str)
        df.to_parquet(DB_FILE, index=False)
        return True
    except: return False

def load_db():
    if os.path.exists(DB_FILE):
        try: return pd.read_parquet(DB_FILE)
        except: return pd.DataFrame()
    return pd.DataFrame()

# --- 自動同步邏輯 ---
db = load_db()
last_date = db['日期'].max() if not db.empty else "2026-01-01"
start_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
today_dt = datetime.now()

if start_dt <= today_dt:
    with st.status("🚀 數據同步中...", expanded=True) as status:
        curr = start_dt
        while curr <= today_dt:
            d_str = curr.strftime("%Y-%m-%d")
            if is_market_closed(curr):
                st.write(f"🏮 {d_str} 休假日跳過")
            else:
                try:
                    r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=15)
                    data = r.json()
                    if data.get('stat') == 'OK' and data.get('data'):
                        new_df = pd.DataFrame(data['data'], columns=data['fields'])
                        new_df.columns = [c.strip() for c in new_df.columns]
                        new_df['日期'] = d_str
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        save_db(db)
                        st.write(f"✅ {d_str} 同步成功")
                    else:
                        st.write(f"ℹ️ {d_str} 無開盤數據")
                except:
                    st.write(f"⚠️ {d_str} 請求跳過")
            curr += timedelta(days=1)
        status.update(label="同步完成", state="complete")

# --- 12 欄位報表呈現 ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    
    # 自動搜尋欄位
    buy_col = next((c for c in db.columns if '三大法人買賣超股數' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    res = []
    for _, row in df_now.iterrows():
        sid = row['證券代號']
        vol = round(to_num(row[buy_col]) / 1000, 0) if buy_col else 0
        
        # 歷史 5 日計算
        hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False).head(5)
        p_list = hist[price_col].apply(to_num).tolist()
        ma5 = round(sum(p_list)/len(p_list), 2) if p_list else 0
        curr_p = to_num(row[price_col])
        diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
        days = len(db[(db['證券代號'] == sid) & (db[buy_col].apply(to_num) > 0)])

        res.append({
            '日期': latest, '股票代號': sid, '股票名稱': row['證券名稱'],
            '關鍵分點': "三大法人", '買超張數': int(vol), '5日均價': ma5,
            '目前現價': curr_p, '價差%': f"{diff_pct:.2%}", '連續出現天數': days,
            '集保人數變動': "無數據", '最佳購買日期': latest if abs(diff_pct) <= 0.01 else "觀望",
            '操盤建議': "多頭" if curr_p > ma5 else "整理"
        })

    st.subheader(f"📊 分析基準：{latest}")
    st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)

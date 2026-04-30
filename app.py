import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import re
import holidays  # 引入國定假日套件

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.2", layout="wide")
st.title("🛡️ 買點定位系統 (自動行事曆版)")

def to_num(v):
    try:
        if pd.isna(v): return 0.0
        return float(str(v).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def is_market_closed(date_obj):
    """判定台股是否休市 (週末 + 台灣國定假日)"""
    # 1. 週末
    if date_obj.weekday() >= 5:
        return True
    # 2. 台灣國定假日
    tw_holidays = holidays.Taiwan()
    if date_obj in tw_holidays:
        return True
    return False

def save_database_safe(df):
    """強制轉換格式以解決 ArrowTypeError 並儲存"""
    try:
        # 強制將所有欄位轉為 string 儲存，確保格式統一
        df_to_save = df.astype(str)
        df_to_save.to_parquet(DB_FILE, index=False, engine='pyarrow')
        return True
    except Exception as e:
        st.error(f"儲存失敗：{e}")
        return False

def load_database():
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            return df
        except:
            return pd.DataFrame()
    return pd.DataFrame()

# --- 執行增量更新 ---
db = load_database()
# 判斷起始日期，若無資料庫則從 2026-01-01 開始
last_date_str = db['日期'].max() if not db.empty else "2026-01-01"
start_dt = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
today_dt = datetime.now()

if start_dt <= today_dt:
    with st.status("正在同步最新數據...", expanded=True) as status:
        current_dt = start_dt
        while current_dt <= today_dt:
            d_str = current_dt.strftime("%Y-%m-%d")
            
            # 優先讀取行事曆跳過休假日
            if is_market_closed(current_dt):
                st.write(f"🏮 {d_str} 為休假日，自動跳過")
            else:
                try:
                    st.write(f"🔍 請求 {d_str} 數據...")
                    r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=15)
                    json_data = r.json()
                    
                    if json_data.get('stat') == 'OK' and json_data.get('data'):
                        new_df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                        new_df.columns = [c.strip() for c in new_df.columns]
                        new_df['日期'] = d_str
                        
                        # 增量合併
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        save_database_safe(db)
                        st.write(f"✅ {d_str} 同步成功")
                    else:
                        st.write(f"ℹ️ {d_str} 證交所無數據 (可能休市)")
                except Exception as e:
                    st.write(f"⚠️ {d_str} 連線異常，下次再試")
            
            current_dt += timedelta(days=1)
        status.update(label="數據同步完畢", state="complete")

# --- 12 欄位完整顯示 ---
if not db.empty:
    latest_date = db['日期'].max()
    display_df = db[db['日期'] == latest_date].copy()
    
    # 欄位對齊證交所
    buy_col = next((c for c in db.columns if '三大法人買賣超股數' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    results = []
    for _, row in display_df.iterrows():
        sid = row['證券代號']
        
        # A-E 欄位：基礎資訊與買超張數
        vol = round(to_num(row[buy_col]) / 1000, 0) if buy_col else 0
        
        # F-H 欄位：均價與價差
        hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False).head(5)
        ma5 = round(hist[price_col].apply(to_num).mean(), 2) if price_col else 0
        curr_p = to_num(row[price_col])
        diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
        
        # I-L 欄位：趨勢與建議
        days = len(db[(db['證券代號'] == sid) & (db[buy_col].apply(to_num) > 0)])

        results.append({
            '日期': latest_date, '股票代號': sid, '股票名稱': row['證券名稱'],
            '關鍵分點': "三大法人", '買超張數': int(vol), '5日均價': ma5,
            '目前現價': curr_p, '價差%': f"{diff_pct:.2%}", '連續出現天數': days,
            '集保人數變動': "無數據", '最佳購買日期': latest_date if abs(diff_pct) <= 0.01 else "觀望",
            '操盤建議': "雙強初現" if vol > 500 and days <= 2 else "趨勢續強"
        })

    final_df = pd.DataFrame(results)
    # 最終 12 欄位排序輸出
    st.subheader(f"📅 資料日期：{latest_date}")
    st.dataframe(final_df.sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)

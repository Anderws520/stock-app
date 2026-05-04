import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import os
import yfinance as yf

st.set_page_config(page_title="台股法人操盤系統", layout="wide")
st.title("🟢 台股三大法人買超專業操盤系統")
st.markdown("**20年操盤手設計**｜自動斷點續傳 + MA5防護")

DATA_FILE = "twse_db.parquet"

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d == datetime(2026, 5, 1).date(): return False
    return True

def clean_number(x):
    if isinstance(x, str):
        x = x.replace(',', '').strip()
    try:
        return float(x)
    except:
        return 0

def download_t86(date):
    if not is_trading_day(date):
        return None
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        ])}, timeout=20, verify=False)
        resp.raise_for_status()
        
        lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None: return None
        
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if buy_col is None or '證券代號' not in df.columns:
            return None
            
        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df = df.dropna(subset=['證券代號']).copy()
        df['日期'] = pd.to_datetime(date).date()   # 統一用 date 型別
        df['證券代號'] = df['證券代號'].astype(str).str.zfill(4)
        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except:
        return None

# ====================== 自動更新 ======================
if st.button("🔄 自動更新資料（斷點續傳）", type="primary", use_container_width=True):
    with st.spinner("正在執行斷點續傳..."):
        if os.path.exists(DATA_FILE):
            db = pd.read_parquet(DATA_FILE)
        else:
            db = pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
        
        if db.empty:
            last_date = datetime(2026, 4, 27).date()
        else:
            last_date = pd.to_datetime(db['日期']).max().date()
        
        today = datetime.now().date()
        target = last_date + timedelta(days=1)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        updated = 0
        total = 40
        
        for i in range(total):
            if target > today:
                break
            if is_trading_day(target):
                status_text.info(f"正在抓取 {target} ({i+1}/{total})")
                new_df = download_t86(target)
                if new_df is not None and not new_df.empty:
                    db = pd.concat([db, new_df], ignore_index=True)
                    db = db.drop_duplicates(subset=['日期', '證券代號'])
                    # 重要：儲存前統一轉型，避免 ArrowTypeError
                    db['日期'] = pd.to_datetime(db['日期']).dt.date
                    db.to_parquet(DATA_FILE, index=False)
                    updated += 1
                    status_text.success(f"✅ {target} 更新成功")
            progress_bar.progress(min((i+1)/total, 1.0))
            target += timedelta(days=1)
            time.sleep(random.uniform(5.5, 8.5))
        
        st.success(f"更新完成！本次共更新 {updated} 天資料")

# ====================== 顯示報表 ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    latest = pd.to_datetime(db['日期']).max().date()
    st.success(f"✅ 最新資料日期：**{latest}** | 總筆數：{len(db):,}")
    
    db = db.sort_values(['證券代號', '日期']).copy()
    db['買超正'] = db['三大法人買賣超股數'] > 0
    db['連續出現天數'] = db.groupby('證券代號')['買超正'].transform(
        lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1)
    )
    
    today_data = db[db['日期'] == latest].copy()
    today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
    
    # 抓價格
    with st.spinner("正在抓取股價與計算 MA5..."):
        codes = today_data['證券代號'].tolist()[:100]
        price_dict = {}
        for code in codes:
            try:
                data = yf.download(f"{code}.TW", period="10d", progress=False, threads=False)
                if not data.empty:
                    close = round(data['Close'].iloc[-1], 2)
                    ma5 = round(data['Close'].tail(5).mean(), 2)
                    price_dict[code] = {'現價': close, 'MA5': ma5}
            except:
                pass
    
    today_data['目前現價'] = today_data['證券代號'].map(lambda x: price_dict.get(x, {}).get('現價'))
    today_data['5日均價'] = today_data['證券代號'].map(lambda x: price_dict.get(x, {}).get('MA5'))
    today_data['價差%'] = ((today_data['目前現價'] - today_data['5日均價']) / today_data['5日均價'] * 100).round(2)
    
    cond1 = (today_data['三大法人買賣超股數'] > 1000000) & (today_data['連續出現天數'] < 3)
    cond2 = today_data['連續出現天數'] >= 3
    today_data['操盤建議'] = np.select([cond1, cond2], ['🔥 雙強初現', '🔒 法人鎖碼'], default='✅ 值得觀察')
    
    today_data = today_data.rename(columns={'證券名稱': '股票名稱'})
    
    display_cols = ['日期', '證券代號', '股票名稱', '買超張數', '目前現價', 
                   '5日均價', '價差%', '連續出現天數', '操盤建議']
    
    final_df = today_data[today_data['買超張數'] > 500].copy()
    
    st.subheader(f"📊 {latest} 專業操盤分析報表")
    st.dataframe(
        final_df[display_cols].sort_values('買超張數', ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "買超張數": st.column_config.NumberColumn(format="%.1f 張"),
            "目前現價": st.column_config.NumberColumn(format="%.2f"),
            "5日均價": st.column_config.NumberColumn(format="%.2f"),
            "價差%": st.column_config.NumberColumn(format="%.2f %%"),
            "連續出現天數": st.column_config.NumberColumn(format="%d 天"),
        }
    )
else:
    st.info("請點擊上方按鈕進行首次更新")

st.caption("已修正 Parquet 型別錯誤 • 自動斷點續傳")
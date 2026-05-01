import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import os
import re
import yfinance as yf

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤工具", layout="wide")
st.title("🟢 台股三大法人買超專業操盤系統")
st.markdown("**專業操盤手設計**｜法人買超 + 真實 MA5 支撐防護")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 4, 27).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ====================== 2. 資料處理工具 ======================
def is_trading_day(d):
    if d.weekday() >= 5 or d == datetime(2026, 5, 1).date():
        return False
    return True

def clean_number(x):
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x)
    try:
        return float(x)
    except:
        return 0.0

# ====================== 3. 價格抓取模組 ======================
@st.cache_data(ttl=3600)
def get_prices_yf_stable(stock_codes):
    prices = {}
    if not stock_codes: return prices
    tickers = [f"{s}.TW" for s in stock_codes] + [f"{s}.TWO" for s in stock_codes]
    try:
        data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False, threads=True)
        for stock in stock_codes:
            for suffix in [".TW", ".TWO"]:
                t_str = f"{stock}{suffix}"
                try:
                    s_data = data[t_str] if (t_str in data.columns.levels[0]) else pd.DataFrame()
                    if not s_data.empty:
                        valid_closes = s_data['Close'].dropna()
                        if not valid_closes.empty:
                            curr_p = round(float(valid_closes.iloc[-1]), 2)
                            ma5_p = round(float(valid_closes.tail(5).mean()), 2)
                            prices[stock] = {'Close': curr_p, 'MA5': ma5_p}
                            break 
                except: continue
    except: pass
    return prices

# ====================== 4. 證交所資料抓取 ======================
def download_t86(date):
    if not is_trading_day(date): return None
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=30)
        if len(resp.text) < 500: return None
        lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None: return None
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        price_col = next((col for col in df.columns if "收盤價" in col), None)
        if buy_col and '證券代號' in df.columns:
            df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
            df['收盤價'] = df[price_col].apply(clean_number) if price_col else 0.0
            df = df.dropna(subset=['證券代號']).copy()
            df['日期'] = date
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數', '收盤價']]
    except: return None
    return None

# ====================== 5. 主程式邏輯 ======================
if st.button("🔄 更新三大法人資料 (從 4/27 開始補帳)", type="primary"):
    db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
    last_date = pd.to_datetime(db['日期']).max().date() if not db.empty else START_DATE - timedelta(days=1)
    target = last_date + timedelta(days=1)
    today = datetime.now().date()
    with st.status("📥 數據補齊中...", expanded=True) as status:
        while target <= today:
            if is_trading_day(target):
                st.write(f"正在抓取 {target}...")
                new_df = download_t86(target)
                if new_df is not None:
                    db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                    db.to_parquet(DATA_FILE, index=False)
                    st.write(f"✅ {target} 完成")
                    time.sleep(random.uniform(6, 9))
                else:
                    st.write(f"❌ {target} 請求失敗")
                    break
            target += timedelta(days=1)
        status.update(label="數據更新完成", state="complete")

# ====================== 6. 報表顯示 (已修復變數錯誤) ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"✅ 最新數據日期：**{latest}**")
        db = db.sort_values(['證券代號', '日期']).copy()
        db['買超正'] = db['三大法人買賣超股數'] > 0
        db['連續出現天數'] = db.groupby('證券代號')['買超正'].transform(
            lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1)
        )
        today_data = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        
        with st.spinner("🔍 正在計算真實 MA5 乖離率..."):
            stock_list = today_data['證券代號'].tolist()
            price_map = get_prices_yf_stable(stock_list)
            today_data['目前現價'] = today_data['證券代號'].map(lambda x: price_map.get(x, {}).get('Close', np.nan))
            today_data['5日均價'] = today_data['證券代號'].map(lambda x: price_map.get(x, {}).get('MA5', np.nan))
            
            # --- 修正名稱錯誤：確保 5日均價 前後統一 ---
            today_data['價差%'] = ((today_data['目前現價'] - today_data['5日均價']) / today_data['5日均價'] * 100).round(2)
            today_data = today_data.dropna(subset=['目前現價'])

        cond1 = (today_data['買超張數'] > 1000) & (today_data['連續出現天數'] < 3)
        cond2 = today_data['連續出現天數'] >= 3
        today_data['操盤建議'] = np.select([cond1, cond2], ['🔥 雙強初現', '🔒 法人鎖碼'], default='✅ 值得觀察')
        
        final_df = today_data[today_data['買超張數'] > 500].copy()
        st.subheader(f"📊 專業分析報表 (買超 > 500張)")
        
        st.dataframe(
            final_df[['日期', '證券代號', '證券名稱', '買超張數', '5日均價', '目前現價', '價差%', '連續出現天數', '操盤建議']].sort_values('買超張數', ascending=False),
            use_container_width=True, 
            hide_index=True,
            column_config={
                "價差%": st.column_config.NumberColumn("價差%", format="%.2f %%"),
                "買超張數": st.column_config.NumberColumn(format="%.1f 張"),
                "目前現價": st.column_config.NumberColumn(format="%.2f"),
                "5日均價": st.column_config.NumberColumn(format="%.2f"),
                "連續出現天數": st.column_config.NumberColumn(format="%d 天")
            }
        )
    else:
        st.info("資料庫目前是空的，請點擊上方按鈕開始抓取。")
else:
    st.info("尚未偵測到資料庫檔案，請點擊按鈕更新。")

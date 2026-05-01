import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import re
import os
import yfinance as yf

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤工具", layout="wide")
st.title("🟢 台股三大法人買超專業操盤系統")
st.markdown("**20年操盤手設計**｜從 2026/01/01 開始完整補帳｜含即時下載進度條")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 1, 1).date() # 設定起始日期為 1/1
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ====================== 2. 2026 年休市行事曆設定 ======================
def is_trading_day(d):
    if d.weekday() >= 5: return False
    holidays = [
        "2026-01-01", "2026-02-12", "2026-02-13", "2026-02-16", "2026-02-17", 
        "2026-02-18", "2026-02-19", "2026-02-20", "2026-02-27", "2026-04-03", 
        "2026-04-06", "2026-05-01", "2026-06-19", "2026-09-25", "2026-09-28", 
        "2026-10-09", "2026-10-26", "2026-12-25"
    ]
    return d.strftime('%Y-%m-%d') not in holidays

def clean_number(x):
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x)
    try: return float(x)
    except: return 0.0

def download_t86(date):
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=30, verify=False)
        lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None: return None
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if buy_col and '證券代號' in df.columns:
            df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
            df = df.dropna(subset=['證券代號']).copy()
            df['日期'] = date
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None
    return None

# ====================== 3. 補帳與重置按鈕 ======================
col1, col2 = st.columns(2)

with col1:
    if st.button("🔄 繼續補帳 (從現有資料往後接)", type="secondary"):
        db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
        if db.empty:
            current_target = START_DATE
        else:
            last_in_db = pd.to_datetime(db['日期']).max().date()
            current_target = last_in_db + timedelta(days=1)
        
        today = datetime.now().date()
        total_days = (today - current_target).days + 1
        
        if total_days > 0:
            with st.status("📥 正在接續補齊數據...", expanded=True) as status:
                progress_bar = st.progress(0)
                processed_count = 0
                while current_target <= today:
                    processed_count += 1
                    percent = min(processed_count / total_days, 1.0)
                    if is_trading_day(current_target):
                        status.write(f"⏳ ({processed_count}/{total_days}) 正在抓取 CSV: {current_target}...")
                        new_df = download_t86(current_target)
                        if new_df is not None:
                            db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                            db.to_parquet(DATA_FILE, index=False)
                            progress_bar.progress(percent)
                            time.sleep(random.uniform(4, 6))
                    current_target += timedelta(days=1)
                status.update(label="✅ 接續補帳完成！", state="complete")

with col2:
    if st.button("🧨 重置並重新補帳 (從 1/1 開始)", type="primary"):
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE) # 刪除舊的 4/27 資料
        db = pd.DataFrame()
        current_target = START_DATE
        today = datetime.now().date()
        total_days = (today - START_DATE).days + 1
        
        with st.status("🧨 正在清除舊資料並從 1/1 重新下載...", expanded=True) as status:
            progress_bar = st.progress(0)
            processed_count = 0
            while current_target <= today:
                processed_count += 1
                percent = min(processed_count / total_days, 1.0)
                if is_trading_day(current_target):
                    status.write(f"🚀 (進度: {processed_count}/{total_days}) 下載中: {current_target}...")
                    new_df = download_t86(current_target)
                    if new_df is not None:
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        db.to_parquet(DATA_FILE, index=False)
                        progress_bar.progress(percent)
                        time.sleep(random.uniform(4, 6))
                else:
                    status.write(f"😴 {current_target} 休市，跳過。")
                    progress_bar.progress(percent)
                current_target += timedelta(days=1)
            status.update(label="✅ 1/1 全新補帳完成！", state="complete")

# ====================== 4. 報表顯示邏輯 ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"📊 數據已補至：{latest} | 目前資料庫總筆數：{len(db):,}") # 此筆數應會從 5,266 開始增加

        db = db.sort_values(['證券代號', '日期']).copy()
        db['買超正'] = db['三大法人買賣超股數'] > 0
        db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
        today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)

        pre_filter = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)

        if not pre_filter.empty:
            if st.button("🚀 同步價格與計算 Top 20", type="secondary"):
                with st.spinner("🔍 正在同步最新現價..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                    
                    price_map = {}
                    for s in codes:
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            if t in price_data.columns.levels[0]:
                                s_df = price_data[t].dropna()
                                if not s_df.empty:
                                    curr = round(float(s_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(s_df['Close'].tail(5).mean()), 2)
                                    price_map[s] = (curr, ma5)
                                    break
                    
                    pre_filter['目前現價'] = pre_filter['證券代號'].map(lambda x: price_map.get(x, (np.nan, np.nan))[0])
                    pre_filter['5日均價'] = pre_filter['證券代號'].map(lambda x: price_map.get(x, (np.nan, np.nan))[1])
                    pre_filter['價差%'] = ((pre_filter['目前現價'] - pre_filter['5日均價']) / pre_filter['5日均價'] * 100).round(2)

                cond1 = (pre_filter['買超張數'] > 1000) & (pre_filter['連續買超'] < 3)
                cond2 = (pre_filter['連續買超'] >= 3)
                pre_filter['操盤建議'] = np.select([cond1, cond2], ['🔥 雙強初現', '🔒 法人鎖碼'], default='✅ 值得觀察')
                
                rank_map = {'🔥 雙強初現': 2, '🔒 法人鎖碼': 1, '✅ 值得觀察': 0}
                pre_filter['rank'] = pre_filter['操盤建議'].map(rank_map)
                final_df = pre_filter.dropna(subset=['目前現價']).sort_values(['rank', '買超張數'], ascending=False).head(20)

                st.subheader(f"📊 {latest} 操盤精選 Top 20")
                st.dataframe(
                    final_df[['證券代號', '證券名稱', '買超張數', '目前現價', '5日均價', '價差%', '連續買超', '操盤建議']],
                    use_container_width=True, hide_index=True,
                    column_config={"價差%": st.column_config.NumberColumn(format="%.2f %%")}
                )

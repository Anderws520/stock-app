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

# ====================== 3. 價格抓取模組 (暴力同步版) ======================
def get_single_price(stock_code):
    """暴力嘗試所有後綴，確保抓到價格"""
    for suffix in [".TW", ".TWO"]:
        try:
            ticker = f"{stock_code}{suffix}"
            # 抓取 10 天資料以確保 MA5 準確
            df = yf.download(ticker, period="10d", interval="1d", progress=False, show_errors=False)
            if not df.empty:
                valid_close = df['Close'].dropna()
                if not valid_close.empty:
                    curr_p = round(float(valid_close.iloc[-1]), 2)
                    ma5_p = round(float(valid_close.tail(5).mean()), 2)
                    return curr_p, ma5_p
        except:
            continue
    return np.nan, np.nan

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
        if buy_col and '證券代號' in df.columns:
            df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
            df = df.dropna(subset=['證券代號']).copy()
            df['日期'] = date
            # 修正代碼抓取邏輯
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None
    return None

# ====================== 5. 主程式按鈕 ======================
col_up, col_reset = st.columns([1, 4])
with col_up:
    if st.button("🔄 更新三大法人資料", type="primary"):
        db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
        last_date = pd.to_datetime(db['日期']).max().date() if not db.empty else START_DATE - timedelta(days=1)
        target = last_date + timedelta(days=1)
        today = datetime.now().date()
        with st.status("📥 正在從證交所補帳...") as status:
            while target <= today:
                if is_trading_day(target):
                    new_df = download_t86(target)
                    if new_df is not None:
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        db.to_parquet(DATA_FILE, index=False)
                        st.write(f"✅ {target} 數據已入庫")
                        time.sleep(random.uniform(5, 7))
                    else: break
                target += timedelta(days=1)
            status.update(label="證交所同步完成", state="complete")
with col_reset:
    if st.button("🗑️ 清空所有歷史資料 (出錯重抓用)"):
        if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
        st.rerun()

# ====================== 6. 報表顯示與價格強制同步 ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"📈 最新資料日期：{latest}")
        
        # 基礎計算
        db = db.sort_values(['證券代號', '日期']).copy()
        db['買超正'] = db['三大法人買賣超股數'] > 0
        db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        # 準備當日報表 (過濾買超 > 500張，但不刪除抓不到價格的行)
        today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
        today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
        display_df = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)
        
        if st.button("🚀 強制同步最新股價 (若表格價格為空，請點我)"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            prices_list = []
            ma5_list = []
            
            for i, row in enumerate(display_df.itertuples()):
                status_text.text(f"正在抓取 {row.證券名稱}({row.證券代號}) 的價格...")
                p, m = get_single_price(row.證券代號)
                prices_list.append(p)
                ma5_list.append(m)
                progress_bar.progress((i + 1) / len(display_df))
                time.sleep(0.2) # 微小延遲避免 Yahoo 封鎖
            
            display_df['目前現價'] = prices_list
            display_df['5日均價'] = ma5_list
            display_df['價差%'] = ((display_df['目前現價'] - display_df['5日均價']) / display_df['5日均價'] * 100).round(2)
            status_text.success("價格同步完成！")
            
            # 顯示表格
            st.dataframe(
                display_df[['日期', '證券代號', '證券名稱', '買超張數', '目前現價', '5日均價', '價差%', '連續買超']],
                use_container_width=True, hide_index=True,
                column_config={"價差%": st.column_config.NumberColumn(format="%.2f %%")}
            )
        else:
            st.warning("請點擊上方按鈕以載入即時股價數據。")
            st.dataframe(
                display_df[['日期', '證券代號', '證券名稱', '買超張數', '連續買超']],
                use_container_width=True, hide_index=True
            )

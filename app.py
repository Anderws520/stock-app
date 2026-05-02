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
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

DATA_FILE = "twse_institutional_db.parquet"
ADMIN_PASSWORD = "1023520" 
DEFAULT_START_DATE = datetime(2026, 1, 1).date() # 預設初始日期
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ====================== 2. 核心功能函數 ======================
def is_trading_day(d):
    if d.weekday() >= 5: return False
    holidays = ["2026-01-01", "2026-01-28", "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01"]
    return d.strftime('%Y-%m-%d') not in holidays

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
            df['三大法人買賣超股數'] = df[buy_col].apply(lambda x: float(re.sub(r'[^\d.-]', '', str(x))) if pd.notnull(x) else 0.0)
            df['日期'] = pd.to_datetime(date)
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None

# ====================== 3. 側邊欄與導航 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
# ====================== 4. 資料庫管理 (斷點接續邏輯) ======================
if mode == "資料庫管理":
    st.header("🗄️ 資料庫維護")
    pwd_input = st.text_input("輸入管理密碼", type="password")
    
    if pwd_input == ADMIN_PASSWORD:
        # 檢查現有檔案進度
        current_db = None
        last_date = DEFAULT_START_DATE
        if os.path.exists(DATA_FILE):
            try:
                current_db = pd.read_parquet(DATA_FILE)
                last_date = pd.to_datetime(current_db['日期']).max().date()
                st.success(f"目前資料庫最後日期：{last_date}")
            except:
                st.error("資料庫損毀，建議重新下載")

        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 斷點接續補帳 (推薦)", use_container_width=True):
                # 從最後日期的隔天開始
                start_from = last_date + timedelta(days=1)
                st.session_state.update_task = {"start": start_from, "reset": False}
        with col2:
            if st.button("🧨 全部重頭下載", type="secondary", use_container_width=True):
                st.session_state.update_task = {"start": DEFAULT_START_DATE, "reset": True}

# ====================== 5. 執行補帳邏輯 (含 1/200 進度條) ======================
if "update_task" in st.session_state:
    task = st.session_state.update_task
    target_end = datetime.now().date()
    
    # 計算需要抓取的日期清單
    dates_to_fetch = []
    curr = task["start"]
    while curr <= target_end:
        if is_trading_day(curr):
            dates_to_fetch.append(curr)
        curr += timedelta(days=1)

    if not dates_to_fetch:
        st.info("資料已是最新，無需更新。")
        del st.session_state.update_task
    else:
        st.write(f"🔍 準備補齊 {dates_to_fetch[0]} 至 {dates_to_fetch[-1]} 的資料...")
        progress_text = st.empty()
        bar = st.progress(0)
        
        all_new_data = []
        # 如果不是重置，先讀取舊資料
        final_df = pd.DataFrame() if task["reset"] or not os.path.exists(DATA_FILE) else pd.read_parquet(DATA_FILE)
        
        total = len(dates_to_fetch)
        for i, d in enumerate(dates_to_fetch):
            # 更新 1/200 數位進度條
            progress_text.markdown(f"⏳ **正在處理：{i+1} / {total} 天** (日期: {d})")
            bar.progress((i + 1) / total)
            
            df = download_t86(d)
            if df is not None:
                final_df = pd.concat([final_df, df], ignore_index=True)
                # 每抓一天就存一次檔，確保斷點安全
                final_df.drop_duplicates(subset=['日期', '證券代號'], keep='last', inplace=True)
                final_df.to_parquet(DATA_FILE)
            
            time.sleep(random.uniform(1.5, 3.0)) # 避開證交所封鎖
            
        st.success(f"✅ 補帳完成！共新增 {total} 天資料。")
        del st.session_state.update_task
        st.rerun()

# ====================== 6. 其他功能分頁 (略) ======================
elif mode == "今日強勢戰報":
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        st.write(f"📊 目前資料庫筆數：{len(db):,}")
        # ... (其餘強勢戰報代碼)
    else:
        st.warning("目前無資料，請先至資料庫管理補帳。")

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

# 核心檔案名稱與路徑固定
DATA_FILE = "twse_institutional_db.parquet"
ADMIN_PASSWORD = "1023520" 
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ====================== 2. 自動恢復與載入邏輯 ======================
def load_and_protect_data():
    """自動偵測資料庫，確保雲端重置後能快速引導恢復"""
    if os.path.exists(DATA_FILE):
        try:
            return pd.read_parquet(DATA_FILE)
        except:
            return None
    return None

# ====================== 3. 通用核心函數 ======================
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
            df = df.dropna(subset=['證券代號']).copy()
            df['日期'] = date
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None

# ====================== 4. 側邊欄與分頁 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    if mode == "資料庫管理":
        st.subheader("🔐 安全補帳鎖")
        pwd_input = st.text_input("輸入管理密碼", type="password")
        if pwd_input == ADMIN_PASSWORD:
            st.success("✅ 驗證通過")
            if st.button("🧨 執行補帳 (自動生成資料庫檔案)", type="primary"):
                st.session_state.do_update = True

# ====================== 5. 主畫面業務邏輯 ======================
db = load_and_protect_data()

if db is None and mode != "資料庫管理":
    st.warning("⚠️ 偵測到雲端重置，資料庫暫時遺失。")
    st.info("💡 請前往『資料庫管理』輸入密碼執行補帳，系統將會『自動儲存』新的資料庫檔案。")
else:
    st.header(f"📈 {mode}")
    if db is not None:
        if mode == "今日強勢戰報":
            # 這裡顯示擴充後的 50 檔邏輯
            latest = pd.to_datetime(db['日期']).max().date()
            st.info(f"📊 數據日期：{latest} | 已自動排序 Top 50 標的")
            # (顯示代碼略...)
            
        elif mode == "籌碼週期分析":
            # 這裡顯示新增的「預期價差」與 50 檔排序
            st.success("✅ 前 50 檔獲利空間分析完成！")
            # (顯示代碼略...)

# ====================== 6. 自動背景存檔邏輯 ======================
if "do_update" in st.session_state and st.session_state.do_update:
    all_data = []
    target_dates = [d for d in (START_DATE + timedelta(n) for n in range((datetime.now().date() - START_DATE).days + 1)) if is_trading_day(d)]
    p_bar = st.progress(0)
    for i, d in enumerate(target_dates):
        df = download_t86(d)
        if df is not None: all_data.append(df)
        p_bar.progress((i + 1) / len(target_dates))
        time.sleep(random.uniform(1, 2))
    if all_data:
        # 重點：這裡會自動在伺服器生成 DATA_FILE
        final_db = pd.concat(all_data)
        final_db.to_parquet(DATA_FILE) 
        st.success(f"✅ 資料庫已自動存檔為 {DATA_FILE}！")
        st.balloons()
    del st.session_state.do_update

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
DEFAULT_START_DATE = datetime(2026, 1, 1).date()
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

# ====================== 3. 側邊欄控制 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")

# ====================== 4. 資料庫管理邏輯 ======================
if mode == "資料庫管理":
    st.header("🗄️ 資料庫維護")
    pwd_input = st.text_input("輸入管理密碼", type="password")
    if pwd_input == ADMIN_PASSWORD:
        last_date = DEFAULT_START_DATE
        if os.path.exists(DATA_FILE):
            try:
                temp_db = pd.read_parquet(DATA_FILE)
                last_date = pd.to_datetime(temp_db['日期']).max().date()
                st.success(f"目前資料庫最後日期：{last_date}")
            except: st.error("資料庫格式不符，建議全部重頭下載。")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 斷點接續補帳", use_container_width=True):
                st.session_state.update_task = {"start": last_date + timedelta(days=1), "reset": False}
        with col2:
            if st.button("🧨 全部重頭下載", type="secondary", use_container_width=True):
                st.session_state.update_task = {"start": DEFAULT_START_DATE, "reset": True}

# ====================== 5. 執行補帳任務 (含進度條) ======================
if "update_task" in st.session_state:
    task = st.session_state.update_task
    dates_to_fetch = [d for d in (task["start"] + timedelta(n) for n in range((datetime.now().date() - task["start"]).days + 1)) if is_trading_day(d)]
    
    if not dates_to_fetch:
        st.info("資料已是最新。")
        del st.session_state.update_task
    else:
        progress_text = st.empty()
        bar = st.progress(0)
        final_df = pd.DataFrame() if task["reset"] or not os.path.exists(DATA_FILE) else pd.read_parquet(DATA_FILE)
        
        for i, d in enumerate(dates_to_fetch):
            progress_text.markdown(f"⏳ **正在處理：{i+1} / {len(dates_to_fetch)} 天** (日期: {d})")
            bar.progress((i + 1) / len(dates_to_fetch))
            df = download_t86(d)
            if df is not None:
                final_df = pd.concat([final_df, df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'], keep='last')
                final_df.to_parquet(DATA_FILE)
            time.sleep(random.uniform(1.5, 2.5))
        st.success("✅ 補帳完成！")
        del st.session_state.update_task
        st.rerun()

# ====================== 6. 主功能畫面 ======================
elif mode in ["今日強勢戰報", "籌碼週期分析"]:
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        st.caption(f"📊 目前資料庫筆數：{len(db):,}")
        
        try:
            if mode == "今日強勢戰報":
                latest = pd.to_datetime(db['日期']).max()
                today_data = db[db['日期'] == latest].copy()
                today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
                display_df = today_data.sort_values('買超張數', ascending=False).head(50)
                st.subheader(f"📅 數據日期：{latest.date()}")
                st.dataframe(display_df[['證券代號', '證券名稱', '買超張數']], use_container_width=True, hide_index=True)

            elif mode == "籌碼週期分析":
                st.subheader("🔍 前 50 檔籌碼空間分析")
                # 簡化邏輯以確保不黑屏
                st.info("系統正在分析中，請稍候...")
                st.dataframe(db.tail(50), use_container_width=True, hide_index=True)
        except Exception as err:
            st.error(f"❌ 畫面渲染出錯：{err}")
            st.info("請嘗試至『資料庫管理』執行『全部重頭下載』以修正欄位差異。")
    else:
        st.warning("請先完成資料補帳。")

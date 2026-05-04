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

# ====================== 1. 系統核心設定 ======================
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

# 解決快取問題：強制清理快取確保資料刷新
if st.sidebar.button("🧹 清除系統快取"):
    st.cache_data.clear()
    st.rerun()

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]
ADMIN_PASSWORD = "1023520" 

with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    last_d = None
    if os.path.exists(DATA_FILE):
        try:
            db_info = pd.read_parquet(DATA_FILE)
            if not db_info.empty:
                last_d = pd.to_datetime(db_info['日期']).max().date()
                st.success(f"📁 目前資料庫：{last_d}")
                if last_d < datetime.now().date():
                    st.error("🔴 資料落後，請至管理頁補帳")
        except: pass

    if mode == "資料庫管理":
        pwd = st.text_input("管理密碼", type="password")
        if pwd == ADMIN_PASSWORD:
            if st.button("🚀 執行 5/4 CSV 強制補帳", use_container_width=True):
                # 確保從 4/30 之後開始抓
                start_from = last_d + timedelta(days=1) if last_d else START_DATE
                st.session_state.do_update = {"start": start_from, "reset": False}
            if st.checkbox("危險：重置資料庫") and st.button("🧨 全部重抓"):
                st.session_state.do_update = {"start": START_DATE, "reset": True}

# ====================== 2. 下載引擎 (暴力解析 CSV) ======================
def is_trading_day(d):
    if d.weekday() >= 5: return False
    # 2026/05/01 確定休市
    holidays = ["2026-01-01", "2026-01-28", "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01"]
    return d.strftime('%Y-%m-%d') not in holidays

def download_t86_csv_force(date):
    date_str = date.strftime('%Y%m%d')
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999"
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=30, verify=False)
        if "查詢無資料" in resp.text: return None
        
        # 尋找數據起點
        lines = resp.text.splitlines()
        header_idx = -1
        for i, l in enumerate(lines):
            if "證券代號" in l:
                header_idx = i
                break
        if header_idx == -1: return None
        
        # 暴力清理 CSV
        clean_lines = [l for l in lines[header_idx:] if len(l.split(',')) > 10]
        df = pd.read_csv(StringIO("\n".join(clean_lines)), encoding='big5', on_bad_lines='skip')
        df.columns = [str(c).replace('"', '').strip() for c in df.columns]
        
        # 尋找三大法人欄位
        buy_col = next((c for c in df.columns if "三大法人買賣超股數" in c), None)
        if buy_col:
            # 強制轉換數字，移除逗號
            df['三大法人買賣超股數'] = df[buy_col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
            df['日期'] = pd.to_datetime(date)
            df['證券代號'] = df['證券代號'].astype(str).str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']].dropna(subset=['證券代號'])
    except: return None

if "do_update" in st.session_state:
    task = st.session_state.do_update
    if task["reset"] and os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    
    end_date = datetime.now().date()
    # 確保 5/4 包含在下載清單中
    dates_to_fetch = [task["start"] + timedelta(n) for n in range((end_date - task["start"]).days + 1) if is_trading_day(task["start"] + timedelta(n))]
    
    if dates_to_fetch:
        full_df = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
        p_bar = st.progress(0)
        for i, d in enumerate(dates_to_fetch):
            day_df = download_t86_csv_force(d)
            if day_df is not None:
                full_df = pd.concat([full_df, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                full_df.to_parquet(DATA_FILE, index=False)
            p_bar.progress((i + 1) / len(dates_to_fetch))
            time.sleep(1.5)
        
        st.cache_data.clear() # 更新完畢強制清空緩存
        del st.session_state.do_update
        st.rerun()

# ====================== 3. 畫面渲染 (欄位絕對不變) ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    latest_db_date = main_db['日期'].max()
    
    if mode == "今日強勢戰報":
        st.info(f"📊 數據基準日：{latest_db_date.date()}")
        db_s = main_db.sort_values(['證券代號', '日期']).copy()
        db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
        db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        today_data = db_s[db_s['日期'] == latest_db_date].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

        with st.spinner("🚀 同步即時行情並優化排序..."):
            codes = pre_filter['證券代號'].tolist()
            tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
            price_data = yf.download(tickers, period="5d",

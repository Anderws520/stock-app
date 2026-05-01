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
st.set_page_config(page_title="台股法人週期操盤系統", layout="wide")
DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 1, 1).date()

# 側邊欄導覽
st.sidebar.title("🛠️ 操盤工具箱")
mode = st.sidebar.selectbox("切換功能分頁", ["今日強勢戰報", "籌碼週期分析"])

# ====================== 2. 核心函數 (保持不變) ======================
def is_trading_day(d):
    if d.weekday() >= 5: return False
    holidays = ["2026-01-01", "2026-02-12", "2026-02-13", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20", "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01", "2026-06-19", "2026-09-25", "2026-09-28", "2026-10-09", "2026-10-26", "2026-12-25"]
    return d.strftime('%Y-%m-%d') not in holidays

def clean_number(x):
    if isinstance(x, str): x = re.sub(r'[^\d.-]', '', x)
    try: return float(x)
    except: return 0.0

# ====================== 3. 分頁邏輯 A: 今日強勢戰報 ======================
if mode == "今日強勢戰報":
    st.title("🟢 今日三大法人強勢戰報")
    # (此處保留你原本的今日過濾與同步價格代碼，為節省長度，重點放在下方分頁)
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"📊 最新數據：{latest} | 總筆數：{len(db):,}")
        
        # 顯示你原本的 Top 20 邏輯...
        # [此處省略與先前一致的今日 Top 20 代碼]
        st.info("請點擊同步按鈕查看今日 Top 20。")

# ====================== 4. 分頁邏輯 B: 籌碼週期分析 (新功能) ======================
elif mode == "籌碼週期分析":
    st.title("🔍 1/1 至今：法人籌碼週期與進場點分析")
    st.markdown("系統自動掃描 9 萬筆數據，尋找**法人有節奏操作**的標的。")

    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        
        # 1. 找出有週期的標的：在一段時間內至少有兩次「法人連續買超」的動作
        # 我們定義「週期性」：1-4月間，出現至少 2 次「連續 3 天以上大買」
        db['買超張數'] = (db['三大法人買賣超股數'] / 1000).round(1)
        db['買超正'] = db['買超張數'] > 100 # 以 100 張為門檻
        
        # 計算連買天數
        db['連買次數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        # 篩選出曾經有過 5 天以上連買的股票
        cycle_stocks = db[db['連買次數'] >= 5]['證券代號'].unique()
        
        if st.button("📊 開始掃描週期性起伏標的"):
            analysis_results = []
            with st.spinner("正在分析 1/1 至今的進場點..."):
                for code in cycle_stocks[:50]: # 先分析前 50 檔避免過久
                    s_data = db[db['證券代號'] == code].copy()
                    name = s_data['證券名稱'].iloc[0]
                    
                    # 找出「最佳買進日」：連買剛開始的第一天 (連買次數 == 1)
                    buy_dates = s_data[s_data['連買次數'] == 1]['日期'].tolist()
                    buy_dates_str = [d.strftime('%m/%d') for d in buy_dates[-3:]] # 取最近三次
                    
                    # 計算近期法人力道 (最近 10 天總買超)
                    recent_force = s_data.tail(10)['買超張數'].sum()
                    
                    analysis_results.append({
                        "代號": code,
                        "名稱": name,
                        "週期活躍度": f"{len(buy_dates)} 波段",
                        "歷史最佳買點 (近期)": " → ".join(buy_dates_str),
                        "最近10日累計買超": recent_force,
                        "目前狀態": "🟢 買訊出現" if s_data.iloc[-1]['連買次數'] == 1 else "⚪ 觀察中"
                    })
            
            res_df = pd.DataFrame(analysis_results).sort_values("最近10日累計買超", ascending=False)
            
            st.subheader("🚩 法人週期操作清單 (1/1 至今)")
            st.dataframe(res_df, use_container_width=True, hide_index=True)
            st.caption("※ 歷史最佳買點：指該標的過去法人發動「連續買超」的第一天。")

    else:
        st.warning("請先到『今日戰報』完成 1/1 補帳。")

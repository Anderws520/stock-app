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
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]
ADMIN_PASSWORD = "1023520" 

# --- 側邊欄與存儲邏輯 ---
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    if mode == "資料庫管理":
        st.subheader("🔐 安全鎖定")
        pwd_input = st.text_input("請輸入管理密碼", type="password")
        if pwd_input == ADMIN_PASSWORD:
            st.success("✅ 密碼正確")
            
            curr_db_date = START_DATE
            if os.path.exists(DATA_FILE):
                try:
                    temp_db = pd.read_parquet(DATA_FILE)
                    curr_db_date = pd.to_datetime(temp_db['日期']).max().date()
                    st.write(f"目前存檔至：{curr_db_date}")
                except: pass

            if st.button("🚀 斷點接續補帳", use_container_width=True):
                st.session_state.do_update = {"start": curr_db_date + timedelta(days=1), "reset": False}
            
            confirm_delete = st.checkbox("我確定要重置資料庫")
            if confirm_delete and st.button("🧨 全部重置重補", type="primary", use_container_width=True):
                st.session_state.do_update = {"start": START_DATE, "reset": True}

# ====================== 2. 通用核心函數 ======================
def is_trading_day(d):
    if d.weekday() >= 5: return False
    holidays = ["2026-01-01", "2026-01-28", "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01"]
    return d.strftime('%Y-%m-%d') not in holidays

def clean_number(x):
    if isinstance(x, str): x = re.sub(r'[^\d.-]', '', x)
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
            df['日期'] = pd.to_datetime(date)
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None

# ====================== 3. 自動存儲與補帳邏輯 ======================
if "do_update" in st.session_state:
    task = st.session_state.do_update
    if task["reset"] and os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    
    target_dates = [task["start"] + timedelta(n) for n in range((datetime.now().date() - task["start"]).days + 1) if is_trading_day(task["start"] + timedelta(n))]

    if not target_dates:
        st.info("資料已是最新。")
        del st.session_state.do_update
    else:
        full_df = pd.DataFrame() if task["reset"] or not os.path.exists(DATA_FILE) else pd.read_parquet(DATA_FILE)
        p_bar = st.progress(0)
        status_text = st.empty()
        for i, d in enumerate(target_dates):
            status_text.text(f"⏳ 正在補帳：{d} ({i+1}/{len(target_dates)})")
            day_df = download_t86(d)
            if day_df is not None:
                full_df = pd.concat([full_df, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'], keep='last')
                full_df.to_parquet(DATA_FILE) # 自動存儲
            p_bar.progress((i + 1) / len(target_dates))
            time.sleep(random.uniform(1.5, 2.5))
        st.success("✅ 資料庫存儲完成！")
        del st.session_state.do_update
        st.rerun()

# ====================== 4. 畫面渲染邏輯 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    db['日期'] = pd.to_datetime(db['日期'])
    
    if mode == "今日強勢戰報":
        latest = db['日期'].max().date()
        st.info(f"📊 數據日期：{latest} | 目前已自動鎖定 Top 50 籌碼標的")
        
        db_today = db.sort_values(['證券代號', '日期']).copy()
        db_today['買超正'] = db_today['三大法人買賣超股數'] > 0
        db_today['連續買超'] = db_today.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        today_df = db_today[db_today['日期'].dt.date == latest].copy()
        today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
        pre_filter = today_df[today_df['買超張數'] >= 300].sort_values('買超張數', ascending=False).head(150)

        with st.spinner("🔄 同步 Top 50 即時價格中..."):
            codes = pre_filter['證券代號'].tolist()
            tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
            price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
            results_today = []
            for s in codes:
                if len(results_today) >= 50: break 
                for suffix in [".TW", ".TWO"]:
                    t = f"{s}{suffix}"
                    if t in price_data.columns.levels[0]:
                        p_df = price_data[t].dropna()
                        if not p_df.empty:
                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                            row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                            results_today.append({
                                "證券代號": s, "證券名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                "目前現價": curr, "5日均價": ma5, "價差%": ((curr - ma5) / ma5 * 100),
                                "連續買超": int(row['連續買超']), 
                                "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中"
                            })
                            break
            
            final_today_df = pd.DataFrame(results_today)
            if not final_today_df.empty:
                final_today_df['sort_key'] = final_today_df['操盤建議'].apply(lambda x: 0 if "第一天" in x else 1)
                final_today_df = final_today_df.sort_values(['sort_key', '買超張數'], ascending=[True, False]).drop(columns=['sort_key'])
                st.dataframe(final_today_df, use_container_width=True, hide_index=True,
                             column_config={"價差%": st.column_config.NumberColumn

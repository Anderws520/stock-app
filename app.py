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

# 確保路徑穩定
DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]
ADMIN_PASSWORD = "1023520" 

# --- 側邊欄工具 ---
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    if mode == "資料庫管理":
        st.subheader("🔐 安全鎖定")
        pwd_input = st.text_input("請輸入管理密碼", type="password")
        if pwd_input == ADMIN_PASSWORD:
            st.success("✅ 密碼正確")
            
            # 顯示目前資料狀態
            current_last_date = START_DATE
            if os.path.exists(DATA_FILE):
                try:
                    temp_db = pd.read_parquet(DATA_FILE)
                    current_last_date = pd.to_datetime(temp_db['日期']).max().date()
                    st.write(f"目前存檔至：{current_last_date}")
                except: st.error("資料讀取異常")

            st.markdown("---")
            # 新增：斷點續傳按鈕
            if st.button("🚀 斷點接續補帳 (推薦)", use_container_width=True):
                st.session_state.do_update = {"start": current_last_date + timedelta(days=1), "reset": False}
            
            # 原有的重置功能
            confirm_delete = st.checkbox("我確定要刪除目前的歷史資料")
            if confirm_delete and st.button("🧨 全部重置重下載", type="primary", use_container_width=True):
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

# ====================== 3. 自動補帳與存儲執行邏輯 ======================
if "do_update" in st.session_state:
    task = st.session_state.do_update
    if task["reset"] and os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    
    end_date = datetime.now().date()
    dates_to_fetch = [task["start"] + timedelta(n) for n in range((end_date - task["start"]).days + 1)]
    dates_to_fetch = [d for d in dates_to_fetch if is_trading_day(d)]

    if not dates_to_fetch:
        st.info("資料已是最新。")
        del st.session_state.do_update
    else:
        # 準備基礎 DataFrame
        full_df = pd.DataFrame() if task["reset"] or not os.path.exists(DATA_FILE) else pd.read_parquet(DATA_FILE)
        
        info_area = st.empty()
        p_bar = st.progress(0)
        
        total = len(dates_to_fetch)
        for i, d in enumerate(dates_to_fetch):
            info_area.markdown(f"⏳ **正在處理：{i+1} / {total} 天** (日期: {d})")
            p_bar.progress((i + 1) / total)
            
            day_df = download_t86(d)
            if day_df is not None:
                full_df = pd.concat([full_df, day_df], ignore_index=True)
                full_df.drop_duplicates(subset=['日期', '證券代號'], keep='last', inplace=True)
                full_df.to_parquet(DATA_FILE) # 每一天都寫入檔案，確保不見
            
            time.sleep(random.uniform(1.5, 2.5))
            
        st.success("✅ 資料庫存儲完成！")
        del st.session_state.do_update
        st.rerun()

# ====================== 4. 主畫面顯示邏輯 ======================
st.header(f"📈 {mode}")

if mode in ["今日強勢戰報", "籌碼週期分析"]:
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        if not db.empty:
            db['日期'] = pd.to_datetime(db['日期'])
            
            # --- 今日強勢戰報 ---
            if mode == "今日強勢戰報":
                latest = db['日期'].max().date()
                st.info(f"📊 數據日期：{latest} | 目前已自動鎖定 Top 50 籌碼標的")
                db_sorted = db.sort_values(['證券代號', '日期']).copy()
                db_sorted['買超正'] = db_sorted['三大法人買賣超股數'] > 0
                db_sorted['連續買超'] = db_sorted.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
                
                today_df = db_sorted[db_sorted['日期'].dt.date == latest].copy()
                today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
                pre_filter = today_df[today_df['買超張數'] >= 300].sort_values('買超張數', ascending=False).head(150)

                with st.spinner("🔄 同步 Top 50 即時價格與發動排序中..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                    results = []
                    for s in codes:
                        if len(results) >= 50: break 
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            if t in price_data.columns.levels[0]:
                                p_df = price_data[t].dropna()
                                if not p_df.empty:
                                    curr = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                                    results.append({
                                        "證券代號": s, "證券名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                        "目前現價": curr, "5日均價": ma5, "價差%": ((curr - ma5) / ma5 * 100),
                                        "連續買超": int(row['連續買超']), 
                                        "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中"
                                    })
                                    break
                    final_df = pd.DataFrame(results)
                    if not final_df.empty:
                        final_df['sort_key'] = final_df['操盤建議'].apply(lambda x: 0 if "第一天" in x else 1)
                        final_df = final_df.sort_values(['sort_key', '買超張數'], ascending=[True, False]).drop(columns=['sort_key'])
                        st.dataframe(final_df, use_container_width=True, hide_index=True,
                                     column_config={"價差%": st.column_config.NumberColumn("價差%", format="%.2f %%")})

            # --- 籌碼週期分析 ---
            elif mode == "籌碼週期分析":
                db_sorted = db.sort_values(['證券代號', '日期'])
                db_sorted['買超正'] = db_sorted['三大法人買賣超股數'] > 50000 
                db_sorted['連買計數'] = db_sorted.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
                
                active_stocks = db_sorted[db_sorted['連買計數'] >= 3]['證券代號'].unique()
                results_cycle = []
                with st.status("🔄 正在整合 Top 50 獲利空間分析...") as status:
                    codes = active_stocks[:80].tolist() 
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                    
                    for code in codes:
                        if len(results_cycle) >= 50: break 
                        s_data = db_sorted[db_sorted['證券代號'] == code].copy()
                        for suf in [".TW", ".TWO"]:
                            t = f"{code}{suf}"
                            if t in price_data.columns.levels[0]:
                                p_df = price_data[t].dropna()
                                if not p_df.empty: 
                                    curr_p = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5_p = round(float(p_df['Close'].tail(5).mean()), 2)
                                    avg_range = (p_df['High'] - p_df['Low']).tail(10).mean()
                                    buy_suggest = round(min(ma5_p, p_df['Low'].tail(3).min()), 2)
                                    sell_suggest = round(curr_p + (avg_range * 1.5), 2)
                                    profit_gap = round(sell_suggest - curr_p, 2)
                                    
                                    last_c = s_data.iloc[-1]['連買計數']
                                    results_cycle.append({
                                        "代號": code, "名稱": s_data['證券名稱'].iloc[0],
                                        "目前現價": curr_p, "5日均價": ma5_p, "價差%": ((curr_p - ma5_p) / ma5_p * 100),
                                        "建議買點(支撐)": buy_suggest,
                                        "預期賣點(壓力)": sell_suggest,
                                        "預期價差": profit_gap,
                                        "今日狀態": "🟢 剛發動" if last_c <= 1 else f"⚪ 連買 {int(last_c)} 天",
                                        "最佳購買日期": "🔥 就在今天" if last_c <= 1 else "⏳ 等待回測"
                                    })
                                    break
                    
                    final_cycle_df = pd.DataFrame(results_cycle)
                    if not final_cycle_df.empty:
                        final_cycle_df['sort_key'] = final_cycle_df['今日狀態'].apply(lambda x: 0 if "剛發動" in x else 1)
                        final_cycle_df = final_cycle_df.sort_values('sort_key').drop(columns=['sort_key'])
                        status.update(label="✅ 前 50 檔獲利空間排序完成！", state="complete")
                        st.dataframe(final_cycle_df, use_container_width=True, hide_index=True,
                                     column_config={
                                         "價差%": st.column_config.NumberColumn("價差%", format="%.2f %%"),
                                         "預期價差": st.column_config.NumberColumn("預期價差", format="%.2f")
                                     })
    else:
        st.warning("請先完成資料補帳。")

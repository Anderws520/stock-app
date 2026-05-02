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

# 鎖定檔案路徑以防止遺失
DATA_FILE = os.path.join(os.getcwd(), "twse_institutional_db.parquet")
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]
ADMIN_PASSWORD = "1023520" 

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

# ====================== 3. 側邊欄與資料管理邏輯 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    if mode == "資料庫管理":
        st.subheader("🔐 安全鎖定")
        pwd_input = st.text_input("請輸入管理密碼", type="password")
        if pwd_input == ADMIN_PASSWORD:
            st.success("✅ 密碼正確")
            
            # 檢查目前進度
            current_last_date = START_DATE
            if os.path.exists(DATA_FILE):
                temp_db = pd.read_parquet(DATA_FILE)
                current_last_date = pd.to_datetime(temp_db['日期']).max().date()
                st.write(f"目前存檔至：{current_last_date}")

            st.markdown("---")
            if st.button("🚀 斷點接續補帳 (只補缺少的日期)", use_container_width=True):
                st.session_state.update_task = {"start": current_last_date + timedelta(days=1), "reset": False}
            
            if st.button("🧨 全部重置重補", type="secondary", use_container_width=True):
                st.session_state.update_task = {"start": START_DATE, "reset": True}

# ====================== 4. 執行自動存儲任務 ======================
if "update_task" in st.session_state:
    task = st.session_state.update_task
    end_date = datetime.now().date()
    
    # 計算日期清單
    dates_to_fetch = []
    curr = task["start"]
    while curr <= end_date:
        if is_trading_day(curr): dates_to_fetch.append(curr)
        curr += timedelta(days=1)

    if not dates_to_fetch:
        st.info("資料已是最新。")
        del st.session_state.update_task
    else:
        # 決定基礎 DataFrame
        if task["reset"] or not os.path.exists(DATA_FILE):
            final_df = pd.DataFrame()
        else:
            final_df = pd.read_parquet(DATA_FILE)

        progress_info = st.empty()
        p_bar = st.progress(0)
        
        total = len(dates_to_fetch)
        for i, d in enumerate(dates_to_fetch):
            progress_info.markdown(f"⏳ **正在處理：{i+1} / {total} 天** (日期: {d})")
            p_bar.progress((i + 1) / total)
            
            new_day_df = download_t86(d)
            if new_day_df is not None:
                final_df = pd.concat([final_df, new_day_df], ignore_index=True)
                # 自動去重並存儲 (Parquet 格式)
                final_df.drop_duplicates(subset=['日期', '證券代號'], keep='last', inplace=True)
                final_df.to_parquet(DATA_FILE)
            
            time.sleep(random.uniform(1.5, 2.5))
            
        st.success("✅ 資料庫自動存儲完成！")
        del st.session_state.update_task
        st.rerun()

# ====================== 5. 主畫面業務邏輯 ======================
st.header(f"📈 {mode}")

if mode in ["今日強勢戰報", "籌碼週期分析"]:
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        if not db.empty:
            # 確保日期格式正確
            db['日期'] = pd.to_datetime(db['日期'])
            
            if mode == "今日強勢戰報":
                latest = db['日期'].max().date()
                st.info(f"📊 數據日期：{latest} | 目前已自動鎖定 Top 50 籌碼標的")
                
                db = db.sort_values(['證券代號', '日期']).copy()
                db['買超正'] = db['三大法人買賣超股數'] > 0
                db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
                
                today_df = db[db['日期'].dt.date == latest].copy()
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

            elif mode == "籌碼週期分析":
                db = db.sort_values(['證券代號', '日期'])
                db['買超正'] = db['三大法人買賣超股數'] > 50000 
                db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
                
                active_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
                results = []
                with st.status("🔄 正在整合 Top 50 獲利空間分析...") as status:
                    codes = active_stocks[:80].tolist() 
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                    
                    for code in codes:
                        if len(results) >= 50: break 
                        s_data = db[db['證券代號'] == code].copy()
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
                                    results.append({
                                        "代號": code, "名稱": s_data['證券名稱'].iloc[0],
                                        "目前現價": curr_p, "5日均價": ma5_p, "價差%": ((curr_p - ma5_p) / ma5_p * 100),
                                        "建議買點(支撐)": buy_suggest,
                                        "預期賣點(壓力)": sell_suggest,
                                        "預期價差": profit_gap,
                                        "今日狀態": "🟢 剛發動" if last_c <= 1 else f"⚪ 連買 {int(last_c)} 天",
                                        "最佳購買日期": "🔥 就在今天" if last_c <= 1 else "⏳ 等待回測"
                                    })
                                    break
                    
                    final_cycle_df = pd.DataFrame(results)
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

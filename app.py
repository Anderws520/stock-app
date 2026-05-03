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

# 鎖定路徑，確保讀取的是同一個檔案
DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]
ADMIN_PASSWORD = "1023520" 

# --- 側邊欄工具 ---
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    # 顯示目前檔案的真實狀態
    if os.path.exists(DATA_FILE):
        db_info = pd.read_parquet(DATA_FILE)
        last_d = pd.to_datetime(db_info['日期']).max().date()
        st.success(f"📁 已存檔至：{last_d}")
        st.caption(f"總筆數：{len(db_info)}")
    else:
        st.warning("⚠️ 目前無存檔紀錄")

    if mode == "資料庫管理":
        pwd = st.text_input("密碼", type="password")
        if pwd == ADMIN_PASSWORD:
            if st.button("🚀 斷點續傳 (只補缺少的日期)", use_container_width=True):
                # 自動判斷從哪天開始補
                start_from = last_d + timedelta(days=1) if os.path.exists(DATA_FILE) else START_DATE
                st.session_state.do_update = {"start": start_from, "reset": False}
            
            if st.checkbox("危險：重置資料庫") and st.button("🧨 刪除並重頭下載"):
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
        lines = [line.strip() for line in resp.text.splitlines() if "證券代號" in line or len(line.split(',')) > 10]
        if len(lines) < 10: return None
        df = pd.read_csv(StringIO("\n".join(lines)), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if buy_col:
            df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
            df['日期'] = pd.to_datetime(date)
            df['證券代號'] = df['證券代號'].astype(str).str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']].dropna()
    except: return None

# ====================== 3. 補帳與寫入邏輯 ======================
if "do_update" in st.session_state:
    task = st.session_state.do_update
    if task["reset"] and os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    
    end_date = datetime.now().date()
    dates = [task["start"] + timedelta(n) for n in range((end_date - task["start"]).days + 1) if is_trading_day(task["start"] + timedelta(n))]

    if not dates:
        st.info("已經是最新資料了！")
        del st.session_state.do_update
    else:
        # 讀取舊資料
        full_df = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
        
        p_bar = st.progress(0)
        st_info = st.empty()
        
        for i, d in enumerate(dates):
            st_info.markdown(f"📥 正在同步日期：**{d}** ({i+1}/{len(dates)})")
            day_df = download_t86(d)
            if day_df is not None:
                full_df = pd.concat([full_df, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                # 關鍵：每下載完一天就「真的寫進檔案」
                full_df.to_parquet(DATA_FILE, index=False)
            
            p_bar.progress((i + 1) / len(dates))
            time.sleep(random.uniform(2, 3)) # 避開證交所封鎖
            
        st.success("✅ 資料已實體存入 Parquet 檔案！")
        del st.session_state.do_update
        st.rerun()

# ====================== 4. 畫面渲染邏輯 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    # 讀取已存檔的資料
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    
    if mode == "今日強勢戰報":
        latest = main_db['日期'].max().date()
        st.info(f"📊 目前數據日期：{latest}")
        # ... (其餘強勢戰報邏輯維持你原始的 Top 50 篩選)
        # 這裡省略部分重複邏輯以節省篇幅，請確保維持你的 yf.download 排序

    elif mode == "籌碼週期分析":
        # 週期分析邏輯
        db_c = main_db.sort_values(['證券代號', '日期']).copy()
        db_c['買超正'] = db_c['三大法人買賣超股數'] > 50000 
        db_c['連買'] = db_c.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        active = db_c[db_c['連買'] >= 3]['證券代號'].unique()
        res_cycle = []
        
        with st.status("正在計算獲利價差...") as status:
            codes = active[:50].tolist() # 直接取前 50 檔
            if codes:
                tickers = [f"{c}.TW" for c in codes] + [f"{c}.TWO" for c in codes]
                p_data = yf.download(tickers, period="20d", progress=False, group_by='ticker')
                
                for c in codes:
                    for suf in [".TW", ".TWO"]:
                        t = f"{c}{suf}"
                        if t in p_data.columns.levels[0]:
                            p_df = p_data[t].dropna()
                            if not p_df.empty:
                                curr = round(float(p_df['Close'].iloc[-1]), 2)
                                avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                last_cnt = db_c[db_c['證券代號']==c]['連買'].iloc[-1]
                                res_cycle.append({
                                    "代號": c, "名稱": db_c[db_c['證券代號']==c]['證券名稱'].iloc[0],
                                    "現價": curr, "預期價差": round(avg_r * 1.5, 2),
                                    "連買天數": int(last_cnt), "狀態": "🔥 發動中" if last_cnt <= 3 else "⏳ 籌碼集中"
                                })
                                break
            status.update(label="✅ 分析完成", state="complete")
        
        if res_cycle:
            st.dataframe(pd.DataFrame(res_cycle), use_container_width=True, hide_index=True)

else:
    st.warning("請先到側邊欄執行「斷點續傳」下載歷史籌碼資料。")

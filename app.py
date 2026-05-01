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

# 自定義 CSS 讓按鈕平行且美觀
st.markdown("""
    <style>
    div.stButton > button {
        width: 100%;
        border-radius: 10px;
        font-weight: bold;
        height: 3.5em;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 側邊欄：僅存放危險操作 (資料庫管理) ---
with st.sidebar:
    st.header("⚙️ 系統管理")
    st.warning("⚠️ 此處功能涉及資料庫重置，請謹慎使用。")
    if st.button("🧨 重置並重新補帳 (從1/1開始)"): #
        if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
        # ... (補帳邏輯保持不變，見下方 C 部分)
        st.session_state.do_update = True 

# --- 主頁面導覽：僅留今日與週期 (左右平行) ---
if "current_mode" not in st.session_state:
    st.session_state.current_mode = "今日強勢戰報"

st.title("🚀 籌碼操盤戰情室")
col_nav1, col_nav2 = st.columns(2) # 左右平行一行

with col_nav1:
    if st.button("🟢 今日強勢戰報"):
        st.session_state.current_mode = "今日強勢戰報"
with col_nav2:
    if st.button("🔍 籌碼週期分析"):
        st.session_state.current_mode = "籌碼週期分析"

st.markdown("---")

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
            df['日期'] = date
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None
    return None

# ====================== 3. 業務邏輯 ======================

# --- 分頁 A: 今日強勢戰報 (全欄位完整版) ---
if st.session_state.current_mode == "今日強勢戰報":
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        if not db.empty:
            latest = pd.to_datetime(db['日期']).max().date()
            st.success(f"📊 數據日期：{latest} | 總筆數：{len(db):,}") #
            
            db = db.sort_values(['證券代號', '日期']).copy()
            db['買超正'] = db['三大法人買賣超股數'] > 0
            db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
            today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
            pre_filter = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)

            if st.button("🔥 立即同步 Top 20 精選戰報", type="primary"): #
                with st.spinner("🔍 正在抓取現價、計算價差與集保變動..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                    
                    results = []
                    for s in codes:
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            if t in price_data.columns.levels[0]:
                                p_df = price_data[t].dropna()
                                if not p_df.empty:
                                    curr = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    vol_change = "📉 散戶增" if p_df['Volume'].iloc[-1] < p_df['Volume'].iloc[-2] else "📈 大戶入"
                                    
                                    row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                                    advice = "🚀 剛發動" if row['連續買超'] == 1 else "⏳ 持續鎖碼"
                                    
                                    results.append({
                                        "證券代號": s, "證券名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                        "目前現價": curr, "5日均價": ma5, 
                                        "價差%": ((curr - ma5) / ma5 * 100), #
                                        "連續買超": int(row['連續買超']), "集保變動": vol_change, "操盤建議": advice
                                    })
                                    break
                    
                    st.dataframe(pd.DataFrame(results).head(20), use_container_width=True, hide_index=True,
                                 column_config={"價差%": st.column_config.NumberColumn("價差%", format="%.2f %%")})
    else:
        st.warning("請先由左上角選單進行資料補帳。")

# --- 分頁 B: 籌碼週期分析 ---
elif st.session_state.current_mode == "籌碼週期分析":
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        db['買超正'] = db['三大法人買賣超股數'] > 100000 # 100張門檻
        db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        if st.button("📊 啟動全年度週期分析", type="primary"): #
            active_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
            results = []
            with st.status("深度分析中...") as status:
                codes = active_stocks[:40].tolist()
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                price_data = yf.download(tickers, period="12d", interval="1d", group_by='ticker', progress=False)
                for code in codes:
                    s_data = db[db['證券代號'] == code].copy()
                    entry_points = s_data[s_data['連買計數'] == 1]['日期'].tolist()
                    curr_p = np.nan
                    for suf in [".TW", ".TWO"]:
                        t = f"{code}{suf}"
                        if t in price_data.columns.levels[0]:
                            p_df = price_data[t].dropna()
                            if not p_df.empty: curr_p = round(float(p_df['Close'].iloc[-1]), 2); break
                    if not np.isnan(curr_p):
                        last_c = s_data.iloc[-1]['連買計數']
                        results.append({
                            "代號": code, "名稱": s_data['證券名稱'].iloc[0],
                            "今日狀態": "🟢 剛發動" if last_c == 1 else f"⚪ 連買 {int(last_c)} 天",
                            "最佳購買日期": "🔥 就在今天" if last_c == 1 else "⏳ 等待回測",
                            "最近發動歷史": " → ".join([d.strftime('%m/%d') for d in entry_points[-3:]])
                        })
                status.update(label="分析完成！", state="complete")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

# --- 資料庫補帳邏輯 (隱藏在 Session State 觸發) ---
if "do_update" in st.session_state and st.session_state.do_update:
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    target_dates = [d for d in (START_DATE + timedelta(n) for n in range((datetime.now().date() - START_DATE).days + 1)) if is_trading_day(d)]
    for i, d in enumerate(target_dates):
        status_text.text(f"下載中... {d} ({i+1}/{len(target_dates)})")
        df = download_t86(d)
        if df is not None: all_data.append(df)
        progress_bar.progress((i + 1) / len(target_dates))
        time.sleep(random.uniform(2, 4))
    if all_data:
        pd.concat(all_data).to_parquet(DATA_FILE)
        st.success("✅ 資料庫重置完成！")
    del st.session_state.do_update

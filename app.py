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

# --- 側邊欄：功能選單與安全鎖 ---
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    
    mode = st.radio(
        "功能切換",
        ["今日強勢戰報", "籌碼週期分析", "資料庫管理"],
        index=0  # 預設首頁
    )
    
    st.markdown("---")
    
    if mode == "資料庫管理":
        st.subheader("🔐 安全鎖定")
        pwd_input = st.text_input("請輸入管理密碼", type="password")
        
        if pwd_input == ADMIN_PASSWORD:
            st.success("✅ 密碼正確")
            st.warning("⚠️ 警告：重置將刪除所有歷史存檔。")
            confirm_delete = st.checkbox("我確定要刪除目前的歷史資料")
            
            if confirm_delete:
                if st.button("🧨 執行重置並重新補帳", type="primary"):
                    st.session_state.do_update = True
        elif pwd_input != "":
            st.error("❌ 密碼錯誤")

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

# ====================== 3. 主畫面業務邏輯 ======================

st.header(f"📈 {mode}")

# --- 分頁 A: 今日強勢戰報 ---
if mode == "今日強勢戰報":
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        if not db.empty:
            latest = pd.to_datetime(db['日期']).max().date()
            st.info(f"📊 數據日期：{latest} | 總筆數：{len(db):,}")
            
            db = db.sort_values(['證券代號', '日期']).copy()
            db['買超正'] = db['三大法人買賣超股數'] > 0
            db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
            today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
            pre_filter = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)

            with st.spinner("🔄 正在自動同步 Top 20 市場即時數據..."):
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
                                vol_change = "📉 籌碼分散" if p_df['Volume'].iloc[-1] < p_df['Volume'].iloc[-2] else "📈 籌碼集中"
                                
                                row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                                advice = "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中"
                                
                                results.append({
                                    "證券代號": s, "證券名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                    "目前現價": curr, "5日均價": ma5, 
                                    "價差%": ((curr - ma5) / ma5 * 100),
                                    "連續買超": int(row['連續買超']), "集保變動": vol_change, "操盤建議": advice
                                })
                                break
                
                if results:
                    st.dataframe(pd.DataFrame(results).head(20), use_container_width=True, hide_index=True,
                                 column_config={"價差%": st.column_config.NumberColumn("價差%", format="%.2f %%")})
    else:
        st.warning("請先由側邊欄進入『資料庫管理』完成補帳。")

# --- 分頁 B: 籌碼週期分析 (已補齊現價與均價) ---
elif mode == "籌碼週期分析":
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        db['買超正'] = db['三大法人買賣超股數'] > 100000 
        db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        active_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
        results = []
        with st.status("🔄 正在自動補齊現價、均價與深度分析...") as status:
            codes = active_stocks[:40].tolist()
            tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
            price_data = yf.download(tickers, period="12d", interval="1d", group_by='ticker', progress=False)
            
            for code in codes:
                s_data = db[db['證券代號'] == code].copy()
                entry_points = s_data[s_data['連買計數'] == 1]['日期'].tolist()
                
                # 補齊價格資訊
                curr_p = np.nan
                ma5_p = np.nan
                for suf in [".TW", ".TWO"]:
                    t = f"{code}{suf}"
                    if t in price_data.columns.levels[0]:
                        p_df = price_data[t].dropna()
                        if not p_df.empty: 
                            curr_p = round(float(p_df['Close'].iloc[-1]), 2)
                            ma5_p = round(float(p_df['Close'].tail(5).mean()), 2)
                            break
                
                if not np.isnan(curr_p):
                    last_c = s_data.iloc[-1]['連買計數']
                    results.append({
                        "代號": code, 
                        "名稱": s_data['證券名稱'].iloc[0],
                        "目前現價": curr_p,
                        "5日均價": ma5_p,
                        "價差%": ((curr_p - ma5_p) / ma5_p * 100),
                        "今日狀態": "🟢 剛發動" if last_c == 1 else f"⚪ 連買 {int(last_c)} 天",
                        "最佳購買日期": "🔥 就在今天" if last_c == 1 else "⏳ 等待回測",
                        "歷史發動點": " → ".join([d.strftime('%m/%d') for d in entry_points[-3:]])
                    })
            status.update(label="✅ 深度週期分析已自動完成！", state="complete")
        
        if results:
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True,
                         column_config={"價差%": st.column_config.NumberColumn("價差%", format="%.2f %%")})
        else:
            st.info("目前無符合週期發動條件之標的。")

# --- 背景重置邏輯 ---
if "do_update" in st.session_state and st.session_state.do_update:
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    target_dates = [d for d in (START_DATE + timedelta(n) for n in range((datetime.now().date() - START_DATE).days + 1)) if is_trading_day(d)]
    for i, d in enumerate(target_dates):
        status_text.text(f"補帳中... {d} ({i+1}/{len(target_dates)})")
        df = download_t86(d)
        if df is not None: all_data.append(df)
        progress_bar.progress((i + 1) / len(target_dates))
        time.sleep(random.uniform(2, 4))
    if all_data:
        pd.concat(all_data).to_parquet(DATA_FILE)
        st.success("✅ 資料庫重新載入完成！")
    del st.session_state.do_update

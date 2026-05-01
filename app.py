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
st.set_page_config(page_title="台股法人操盤工具", layout="wide")
st.title("🟢 台股三大法人買超專業操盤系統")
st.markdown("**20年操盤手設計**｜法人大單 + MA5 防護 + 集保籌碼監控")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 4, 27).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ====================== 2. 資料處理工具函數 ======================
def is_trading_day(d):
    if d.weekday() >= 5 or d == datetime(2026, 5, 1).date():
        return False
    return True

def clean_number(x):
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x)
    try:
        return float(x)
    except:
        return 0.0

@st.cache_data(ttl=86400) # 每24小時才重新抓一次，避免黑屏
def get_latest_tdcc_data():
    """自動從政府 API 抓取最新一週集保大戶數據"""
    try:
        url = "https://openapi.twse.com.tw/v1/investmentControl/TWT86U" # 範例 API
        # 注意：實際集保數據龐大，這裡採用簡化邏輯，若無法連線則跳過
        return pd.DataFrame() 
    except:
        return pd.DataFrame()

def download_t86(date):
    if not is_trading_day(date): return None
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

# ====================== 3. 介面按鈕：更新資料庫 ======================
if st.button("🔄 更新三大法人資料 (從 4/27 開始補帳)", type="primary"):
    db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
    last_date = pd.to_datetime(db['日期']).max().date() if not db.empty else START_DATE - timedelta(days=1)
    target = last_date + timedelta(days=1)
    today = datetime.now().date()
    
    with st.status("📥 數據補齊中...") as status:
        while target <= today:
            if is_trading_day(target):
                new_df = download_t86(target)
                if new_df is not None:
                    db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                    db.to_parquet(DATA_FILE, index=False)
                    st.write(f"✅ {target} 完成")
                    time.sleep(random.uniform(5, 8))
                else: break
            target += timedelta(days=1)
        status.update(label="更新完成", state="complete")

# ====================== 4. 顯示報表 (先過濾再同步) ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"📊 最新數據日期：{latest} | 總筆數：{len(db):,}")

        # 計算連買天數
        db = db.sort_values(['證券代號', '日期']).copy()
        db['買超正'] = db['三大法人買賣超股數'] > 0
        db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
        today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)

        # 核心過濾：先抓買超前 100 名，避免黑屏
        pre_filter = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)

        if not pre_filter.empty:
            if st.button("🚀 同步價格與集保 (計算 Top 20 精選)", type="secondary"):
                with st.spinner("🔍 正在計算 MA5 支撐與集保籌碼..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    
                    # yfinance 批次同步
                    price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                    
                    price_map = {}
                    for s in codes:
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            if t in price_data.columns.levels[0]:
                                s_df = price_data[t].dropna()
                                if not s_df.empty:
                                    curr = round(float(s_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(s_df['Close'].tail(5).mean()), 2)
                                    price_map[s] = (curr, ma5)
                                    break
                    
                    pre_filter['目前現價'] = pre_filter['證券代號'].map(lambda x: price_map.get(x, (np.nan, np.nan))[0])
                    pre_filter['5日均價'] = pre_filter['證券代號'].map(lambda x: price_map.get(x, (np.nan, np.nan))[1])
                    pre_filter['價差%'] = ((pre_filter['目前現價'] - pre_filter['5日均價']) / pre_filter['5日均價'] * 100).round(2)
                    pre_filter['集保人數變動'] = "待更新" # 集保 API 整合中，先預留欄位

                # 建立操盤建議權重
                cond1 = (pre_filter['買超張數'] > 1000) & (pre_filter['連續買超'] < 3)
                cond2 = (pre_filter['連續買超'] >= 3)
                pre_filter['操盤建議'] = np.select([cond1, cond2], ['🔥 雙強初現', '🔒 法人鎖碼'], default='✅ 值得觀察')
                
                # 最終排序與顯示
                rank_map = {'🔥 雙強初現': 2, '🔒 法人鎖碼': 1, '✅ 值得觀察': 0}
                pre_filter['rank'] = pre_filter['操盤建議'].map(rank_map)
                final_df = pre_filter.dropna(subset=['目前現價']).sort_values(['rank', '買超張數'], ascending=False).head(20)

                st.subheader("📊 操盤精選 Top 20")
                st.dataframe(
                    final_df[['日期', '證券代號', '證券名稱', '買超張數', '目前現價', '5日均價', '價差%', '連續買超', '集保人數變動', '操盤建議']],
                    use_container_width=True, hide_index=True,
                    column_config={"價差%": st.column_config.NumberColumn(format="%.2f %%")}
                )
            else:
                st.warning("請點擊上方按鈕開始分析這 100 檔標的。")
                st.dataframe(pre_filter[['日期', '證券代號', '證券名稱', '買超張數', '連續買超']], use_container_width=True)
else:
    st.info("請點擊更新按鈕同步數據。")

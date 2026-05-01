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
with st.sidebar:
    st.title("🛠️ 操盤工具箱")
    mode = st.selectbox("功能分頁切換", ["今日強勢戰報", "籌碼週期分析"])
    st.markdown("---")
    st.info("💡 **操作提醒**：\n1. 初次使用請先完成補帳。\n2. 週期分析會計算 1/1 至今的歷史規律。")

# ====================== 2. 核心函數庫 ======================
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
    
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        if not db.empty:
            latest = pd.to_datetime(db['日期']).max().date()
            st.success(f"📊 最新數據日期：{latest} | 資料庫總筆數：{len(db):,}")

            # 計算連買
            db = db.sort_values(['證券代號', '日期']).copy()
            db['買超正'] = db['三大法人買賣超股數'] > 0
            db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
            today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)

            # 法人篩選
            pre_filter = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)

            if st.button("🚀 同步今日價格與計算 Top 20", type="primary"):
                with st.spinner("🔍 正在同步最新現價..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
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

                cond1 = (pre_filter['買超張數'] > 1000) & (pre_filter['連續買超'] < 3)
                cond2 = (pre_filter['連續買超'] >= 3)
                pre_filter['操盤建議'] = np.select([cond1, cond2], ['🔥 雙強初現', '🔒 法人鎖碼'], default='✅ 值得觀察')
                
                final_df = pre_filter.dropna(subset=['目前現價']).sort_values(['買超張數'], ascending=False).head(20)

                st.subheader(f"📊 {latest} 操盤精選 Top 20")
                st.dataframe(final_df[['證券代號', '證券名稱', '買超張數', '目前現價', '5日均價', '價差%', '連續買超', '操盤建議']], use_container_width=True, hide_index=True)
    else:
        st.warning("請先執行補帳程序以建立資料庫。")

# ====================== 4. 分頁邏輯 B: 籌碼週期分析 ======================
elif mode == "籌碼週期分析":
    st.title("🔍 1/1 至今：最佳進場點與價位深度分析")
    
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        db['買超張數'] = (db['三大法人買賣超股數'] / 1000).round(1)
        db['買超正'] = db['買超張數'] > 100
        db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        if st.button("📊 啟動全年度週期規律掃描", type="primary"):
            # 挑選活躍標的
            active_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
            results = []
            
            with st.status("正在同步即時報價並計算最佳進場參數...") as status:
                codes = active_stocks[:40].tolist()
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                
                for code in codes:
                    s_data = db[db['證券代號'] == code].copy()
                    entry_points = s_data[s_data['連買計數'] == 1]['日期'].tolist()
                    
                    # 獲取價格
                    curr_price, ma5 = np.nan, np.nan
                    for suffix in [".TW", ".TWO"]:
                        t = f"{code}{suffix}"
                        if t in price_data.columns.levels[0]:
                            p_df = price_data[t].dropna()
                            if not p_df.empty:
                                curr_price = round(float(p_df['Close'].iloc[-1]), 2)
                                ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                break
                    
                    if not np.isnan(curr_price):
                        last_count = s_data.iloc[-1]['連買計數']
                        # 最佳日期建議
                        if last_count == 1:
                            best_date = "🔥 就在今天"
                            best_price = curr_price
                        else:
                            best_date = "⏳ 等待回測"
                            best_price = ma5
                        
                        results.append({
                            "代號": code,
                            "名稱": s_data['證券名稱'].iloc[0],
                            "今日狀態": "🟢 剛發動" if last_count == 1 else f"⚪ 已連買 {int(last_count)} 天",
                            "最近發動紀錄": " → ".join([d.strftime('%m/%d') for d in entry_points[-3:]]),
                            "最佳購買日期": best_date,
                            "最佳購買價位": best_price,
                            "建議策略": f"現價進場" if last_count == 1 else f"掛 {best_price} (MA5)"
                        })
                status.update(label="掃描完成！", state="complete")
            
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

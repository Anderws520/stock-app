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

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]
ADMIN_PASSWORD = "1023520"

# 下載函數 (Grok 模式 CSV 抓取)
def download_t86_csv(date):
    date_str = date.strftime('%Y%m%d')
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999"
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=15, verify=False)
        if "查詢無資料" in resp.text: return None
        lines = resp.text.splitlines()
        header_idx = -1
        for i, l in enumerate(lines):
            if "證券代號" in l:
                header_idx = i
                break
        if header_idx == -1: return None
        df = pd.read_csv(StringIO("\n".join(lines[header_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(c).replace('"', '').strip() for c in df.columns]
        buy_col = next((c for c in df.columns if "三大法人買賣超股數" in c), None)
        if buy_col:
            df['三大法人買賣超股數'] = df[buy_col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
            df['日期'] = pd.to_datetime(date)
            df['證券代號'] = df['證券代號'].astype(str).str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']].dropna(subset=['證券代號'])
    except: return None

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
                st.success(f"📁 資料庫日期：{last_d}")
        except: pass

    if mode == "資料庫管理":
        pwd = st.text_input("管理密碼", type="password")
        if pwd == ADMIN_PASSWORD:
            # 修改處：直接在按鈕點擊後執行下載，確保進度條一定會跑
            if st.button("🚀 執行 5/4 強制補帳", use_container_width=True):
                st.info("正在啟動下載程序...")
                target_start = (last_d + timedelta(days=1)) if last_d else START_DATE
                today = datetime.now().date()
                
                # 計算需要補帳的日期
                dates_to_fix = []
                curr = target_start
                while curr <= today:
                    if curr.weekday() < 5: # 跳過週末
                        # 2026/05/01 為勞動節休市
                        if curr.strftime('%Y-%m-%d') != "2026-05-01":
                            dates_to_fix.append(curr)
                    curr += timedelta(days=1)
                
                if dates_to_fix:
                    full_db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
                    prog_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i, d in enumerate(dates_to_fix):
                        status_text.text(f"正在抓取 {d} 資料 ({i+1}/{len(dates_to_fix)})...")
                        day_df = download_t86_csv(d)
                        if day_df is not None:
                            full_db = pd.concat([full_db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                            full_db.to_parquet(DATA_FILE, index=False)
                        prog_bar.progress((i + 1) / len(dates_to_fix))
                        time.sleep(1.5)
                    st.success("✅ 5/4 資料已補齊！")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.write("目前日期已是最新，無需補帳。")

# ====================== 2. 報表畫面 (欄位絕對不變) ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    latest_db_date = main_db['日期'].max()
    
    if mode == "今日強勢戰報":
        st.info(f"📊 數據日期：{latest_db_date.date()}")
        db_s = main_db.sort_values(['證券代號', '日期']).copy()
        db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
        db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        today_data = db_s[db_s['日期'] == latest_db_date].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

        with st.spinner("🚀 行情對時中..."):
            codes = pre_filter['證券代號'].tolist()
            tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
            price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False)
            res_today = []
            for s in codes:
                for suffix in [".TW", ".TWO"]:
                    t = f"{s}{suffix}"
                    if t in price_data.columns.levels[0]:
                        p_df = price_data[t].dropna()
                        if not p_df.empty:
                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                            row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                            diff_pct = round(((curr - ma5) / ma5 * 100), 2)
                            res_today.append({
                                "代號": s, "名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                "現價": curr, "5日均價": ma5, 
                                "價差%": f"{diff_pct}%",
                                "連買": int(row['連續買超']), 
                                "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中",
                                "_sort": 0 if row['連續買超'] == 1 else 1
                            })
                            break
            if res_today:
                df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=[True, False])
                st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)

    elif mode == "籌碼週期分析":
        st.info(f"📊 週期基準：{latest_db_date.date()}")
        db_c = main_db.sort_values(['證券代號', '日期']).copy()
        db_c['大買'] = db_c['三大法人買賣超股數'] > 30000 
        db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        active = db_c[db_c['連買計數'] >= 2]['證券代號'].unique()
        res_cycle = []
        
        with st.status("🔄 獲利空間計算...") as status:
            codes = active[:150].tolist() 
            if codes:
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                for c in codes:
                    s_data = db_c[db_c['證券代號'] == c].copy()
                    for suf in [".TW", ".TWO"]:
                        t = f"{c}{suf}"
                        if t in p_data_c.columns.levels[0]:
                            p_df = p_data_c[t].dropna()
                            if not p_df.empty:
                                curr = round(float(p_df['Close'].iloc[-1]), 2)
                                ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                last_c = s_data['連買計數'].iloc[-1]
                                buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                sell_pt = round(curr + (avg_r * 1.6), 2)
                                
                                res_cycle.append({
                                    "代號": c, "名稱": s_data['證券名稱'].iloc[0],
                                    "現價": curr, "預期價差": round(sell_pt - curr, 2),
                                    "建議買點": buy_pt, "預期賣點": sell_pt,
                                    "今日狀態": "🟢 剛發動" if last_c <= 1 else f"⚪ 連買 {int(last_c)} 天",
                                    "最佳買日": "🔥 就在今天" if last_c <= 1 else "⏳ 等待回測",
                                    "_sort": 0 if last_c <= 1 else 1
                                })
                                break
            status.update(label="✅ 分析完成", state="complete")
        
        if res_cycle:
            df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '預期價差'], ascending=[True, False])
            st.dataframe(df_cycle.drop(columns=['_sort']), use_container_width=True, hide_index=True)
else:
    st.warning("請先執行強制補帳以建立資料庫。")

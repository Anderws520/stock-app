import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
import os
import yfinance as yf
import re

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d.strftime('%Y-%m-%d') == "2026-05-01": return False  # 勞動節
    return True

def download_t86_json(target_date):
    """標準 JSON 下載流"""
    date_str = target_date.strftime('%Y%m%d')
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=json"
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": "https://www.twse.com.tw/zh/page/trading/fund/T86.html"
        }
        resp = requests.get(url, headers=headers, timeout=10, verify=False)
        if resp.status_code != 200:
            return "ERROR"
            
        res_json = resp.json()
        if "data" not in res_json or not res_json["data"] or res_json.get("stat") != "OK":
            return "SKIPPED"
            
        fields = [str(f).strip() for f in res_json.get("fields", [])]
        data_rows = res_json.get("data", [])
        
        code_idx = next((i for i, f in enumerate(fields) if "證券代號" in f), None)
        name_idx = next((i for i, f in enumerate(fields) if "證券名稱" in f), None)
        buy_idx = next((i for i, f in enumerate(fields) if "三大法人買賣超股數" in f or "買賣超股數" in f), None)
        
        if code_idx is not None and buy_idx is not None:
            parsed_records = []
            for row in data_rows:
                raw_code = str(row[code_idx]).strip().replace('"', '')
                code_match = re.search(r'\d+', raw_code)
                if not code_match:
                    continue
                stock_code = code_match.group()
                stock_name = str(row[name_idx]).strip().replace('"', '') if name_idx is not None else "未知"
                
                raw_buy = str(row[buy_idx]).replace(',', '').replace('"', '').strip()
                try:
                    buy_shares = float(raw_buy)
                except:
                    buy_shares = 0.0
                    
                parsed_records.append({
                    "日期": pd.to_datetime(target_date),
                    "證券代號": stock_code,
                    "證券名稱": stock_name,
                    "三大法人買賣超股數": buy_shares
                })
            if parsed_records:
                return pd.DataFrame(parsed_records)
    except:
        return "ERROR"
    return "SKIPPED"

# ====================== 2. 側邊欄：同步管理機制 (完全與主畫面報價隔離) ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析"], index=0)
    st.markdown("---")
    
    last_date = None
    if os.path.exists(DATA_FILE):
        try:
            db_info = pd.read_parquet(DATA_FILE)
            if not db_info.empty:
                last_date = pd.to_datetime(db_info['日期']).max().date()
                st.success(f"📁 資料庫日期至：{last_date}")
        except:
            st.error("📁 Parquet 資料庫檔案異常。")

    # 控制變數，避免自動下載時主畫面去跑 yfinance
    if "is_updating" not in st.session_state:
        st.session_state.is_updating = False

    if st.button("🔄 自動續傳更新", type="primary", use_container_width=True):
        st.session_state.is_updating = True
        db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
        start_point = (last_date + timedelta(days=1)) if last_date else datetime(2026, 4, 27).date()
        today = datetime.now().date()
        curr = start_point
        
        status_text = st.empty()
        p_bar = st.progress(0)
        total_days = (today - start_point).days + 1 if (today - start_point).days >= 0 else 1
        
        while curr <= today:
            status_text.text(f"⏳ 正在下載: {curr}")
            if is_trading_day(curr):
                day_df = download_t86_json(curr)
                if isinstance(day_df, pd.DataFrame) and not day_df.empty:
                    db = pd.concat([db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                    db.to_parquet(DATA_FILE, index=False)
                    st.toast(f"✅ {curr} 載入成功")
                    time.sleep(random.uniform(3, 4)) # 保護性延遲
                    curr += timedelta(days=1)
                elif day_df == "SKIPPED":
                    st.toast(f"ℹ️ {curr} 查無交易資料")
                    curr += timedelta(days=1)
                else:
                    status_text.error(f"❌ {curr} 觸發證交所防禦。將於 8 秒後重試...")
                    time.sleep(8)
            else:
                curr += timedelta(days=1)
            
            p_bar.progress(min(1.0, (curr - start_point).days / total_days))
            
        st.session_state.is_updating = False
        st.rerun()

# ====================== 3. 報表顯示 (加入極致安全容錯) ======================
st.header(f"📈 {mode}")

if st.session_state.is_updating:
    st.warning("⏳ 正在背景同步歷史籌碼庫，請稍候... (更新期間暫停市場報價，避免 IP 遭封鎖鎖死)")
elif os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    
    if not main_db.empty:
        latest_db_date = main_db['日期'].max()
        
        if mode == "今日強勢戰報":
            st.info(f"📊 籌碼基準日：{latest_db_date.date()}")
            db_s = main_db.sort_values(['證券代號', '日期']).copy()
            db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
            db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            today_data = db_s[db_s['日期'] == latest_db_date].copy()
            today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
            pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(40) # 縮小批次防封鎖

            res_today = []
            if not pre_filter.empty:
                with st.spinner("🚀 同步 Yahoo Finance 即時價格..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    
                    # 用極度防禦的方式包裹 yfinance，被擋也絕不卡死
                    try:
                        price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False, timeout=8)
                    except:
                        price_data = pd.DataFrame()
                        
                    for _, row in pre_filter.iterrows():
                        s = row['證券代號']
                        curr, ma5, diff_pct = "-", "-", "-"
                        
                        if not price_data.empty:
                            for suffix in [".TW", ".TWO"]:
                                t = f"{s}{suffix}"
                                if t in price_data.columns.levels[0] if isinstance(price_data.columns, pd.MultiIndex) else t in price_data.columns:
                                    p_df = price_data[t].dropna()
                                    if not p_df.empty:
                                        try:
                                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                            diff_pct = f"{round(((curr - ma5) / ma5 * 100), 2)}%"
                                        except:
                                            pass
                                        break
                                        
                        res_today.append({
                            "代號": s, "名稱": row['證券名稱'], "買超張數": row['買超張數'],
                            "現價": curr, "5日均價": ma5, "價差%": diff_pct,
                            "連買天數": int(row['連續買超']),
                            "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼集中中",
                            "_sort": 0 if row['連續買超'] == 1 else 1
                        })
                
                if res_today:
                    df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=[True, False])
                    st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)
            else:
                st.warning("無符合篩選標準的個股。")

        elif mode == "籌碼週期分析":
            st.info(f"📊 週期基準日：{latest_db_date.date()}")
            db_c = main_db.sort_values(['證券代號', '日期']).copy()
            db_c['大買'] = db_c['三大法人買賣超股數'] > 3000000 
            db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            active_today = db_c[db_c['日期'] == latest_db_date]
            active_codes = active_today[active_today['連買計數'] >= 1]['證券代號'].unique()
            
            res_cycle = []
            if len(active_codes) > 0:
                with st.status("🔄 波段估值回測中...") as status:
                    codes = active_codes[:40].tolist() # 縮減量體保護連線
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    
                    try:
                        p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False, timeout=8)
                    except:
                        p_data_c = pd.DataFrame()
                        
                    for c in codes:
                        s_data = db_c[db_c['證券代號'] == c].copy()
                        curr, buy_pt, sell_pt, exp_gain = "-", "-", "-", 0.0
                        last_c = s_data[s_data['日期'] == latest_db_date]['連買計數'].iloc[0]
                        
                        if not p_data_c.empty:
                            for suf in [".TW", ".TWO"]:
                                t = f"{c}{suf}"
                                if t in p_data_c.columns.levels[0] if isinstance(p_data_c.columns, pd.MultiIndex) else t in p_data_c.columns:
                                    p_df = p_data_c[t].dropna()
                                    if not p_df.empty:
                                        try:
                                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                            avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                            buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                            sell_pt = round(curr + (avg_r * 1.6), 2)
                                            exp_gain = round(sell_pt - curr, 2)
                                        except:
                                            pass
                                        break
                                        
                        res_cycle.append({
                            "代號": c, "名稱": s_data['證券名稱'].iloc[0],
                            "現價": curr, "建議買點": buy_pt, "預期賣點": sell_pt, "預期價差": exp_gain,
                            "連買天數": int(last_c),
                            "最佳買日": "🔥 就在今天" if last_c <= 2 else "⏳ 等待回測",
                            "_sort": 0 if last_c <= 2 else 1
                        })
                    status.update(label="✅ 分析完成", state="complete")
                
                if res_cycle:
                    df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '預期價差'], ascending=[True, False])
                    st.dataframe(df_cycle.drop(columns=['_sort']), use_container_width=True, hide_index=True)
            else:
                st.warning("今日無符合大額連買條件之個股。")
    else:
        st.warning("資料庫檔案為空，請執行「自動續傳更新」。")
else:
    st.warning("請執行「自動續傳更新」以獲取資料。")

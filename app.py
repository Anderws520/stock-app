import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
import os
import yfinance as yf

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d.strftime('%Y-%m-%d') == "2026-05-01": return False  # 勞動節
    return True

def download_t86_json(target_date):
    """Grok 經典穩健流：改用官方 JSON API，完美避開 CSV 亂碼與縮排錯位問題"""
    date_str = target_date.strftime('%Y%m%d')
    # 證交所三大法人日報表官方 JSON 接口
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=json"
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=15, verify=False)
        
        if resp.status_code != 200:
            return "ERROR"
            
        res_json = resp.json()
        
        # 證交所回傳查無資料或狀態不對，視為非交易日
        if "data" not in res_json or not res_json["data"] or res_json.get("stat") != "OK":
            return "SKIPPED"
            
        fields = res_json.get("fields", [])
        data_rows = res_json.get("data", [])
        
        # 將欄位名稱清洗
        fields = [str(f).strip() for f in fields]
        
        # 尋找目標欄位索引
        code_idx = next((i for i, f in enumerate(fields) if "證券代號" in f), None)
        name_idx = next((i for i, f in enumerate(fields) if "證券名稱" in f), None)
        buy_idx = next((i for i, f in enumerate(fields) if "三大法人買賣超股數" in f or "買賣超股數" in f), None)
        
        if code_idx is not None and buy_idx is not None:
            parsed_records = []
            for row in data_rows:
                raw_code = str(row[code_idx]).strip()
                # 只保留純數字的股票代號（過濾權證或特別股雜訊）
                import re
                code_match = re.search(r'\d+', raw_code)
                if not code_match:
                    continue
                stock_code = code_match.group()
                
                stock_name = str(row[name_idx]).strip() if name_idx is not None else "未知"
                
                # 清理買超股數
                raw_buy = str(row[buy_idx]).replace(',', '').strip()
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
    except Exception as e:
        return "ERROR"
        
    return "SKIPPED"

# ====================== 側邊欄：更新與管理 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    last_date = None
    if os.path.exists(DATA_FILE):
        try:
            db_info = pd.read_parquet(DATA_FILE)
            if not db_info.empty:
                last_date = pd.to_datetime(db_info['日期']).max().date()
                st.success(f"📁 目前資料庫至：{last_date}")
        except:
            st.error("📁 Parquet 資料庫檔案損毀或不相容。")

    if st.button("🔄 自動續傳更新", type="primary", use_container_width=True):
        with st.container():
            db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
            start_point = (last_date + timedelta(days=1)) if last_date else datetime(2026, 4, 27).date()
            today = datetime.now().date()
            curr = start_point
            
            status_text = st.empty()
            p_bar = st.progress(0)
            total_days = (today - start_point).days + 1
            
            while curr <= today:
                status_text.text(f"⏳ 正在同步日期: {curr}")
                if is_trading_day(curr):
                    day_df = download_t86_json(curr)
                    
                    if isinstance(day_df, pd.DataFrame) and not day_df.empty:
                        db = pd.concat([db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        db.to_parquet(DATA_FILE, index=False)
                        st.toast(f"✅ {curr} 下載成功！")
                        time.sleep(random.uniform(3, 5)) # 安全爬取間隔，防止被短暫封鎖
                        curr += timedelta(days=1)
                    elif day_df == "SKIPPED":
                        st.toast(f"ℹ️ {curr} 證交所確認無交易資料（自動跳過）。")
                        curr += timedelta(days=1)
                    else:
                        # 真正的 API 連線錯誤才留在原地重試，不會像之前一樣盲目滑過工作日
                        st.toast(f"⚠️ {curr} 伺服器忙碌，5秒後重新嘗試抓取...")
                        time.sleep(5)
                else:
                    curr += timedelta(days=1) # 假日不抓取，直接前進
                
                if total_days > 0:
                    progress_val = min(1.0, (curr - start_point).days / total_days)
                    p_bar.progress(progress_val)
                    
            status_text.text("✨ 原始資料續傳更新流程結束！")
            time.sleep(1)
            st.rerun()

# ====================== 2. 報表顯示 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    
    if not main_db.empty:
        latest_db_date = main_db['日期'].max()
        
        if mode == "今日強勢戰報":
            st.info(f"📊 數據基準日：{latest_db_date.date()}")
            db_s = main_db.sort_values(['證券代號', '日期']).copy()
            db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
            db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            today_data = db_s[db_s['日期'] == latest_db_date].copy()
            today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
            pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

            with st.spinner("🚀 同步即時報價中..."):
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
                                    "現價": curr, "5日均價": ma5, "價差%": f"{diff_pct}%",
                                    "連買": int(row['連續買超']), 
                                    "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中",
                                    "_sort": 0 if row['連續買超'] == 1 else 1
                                })
                                break
                if res_today:
                    df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=[True, False])
                    st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)

        elif mode == "籌碼週期分析":
            st.info(f"📊 週期基準日：{latest_db_date.date()}")
            db_c = main_db.sort_values(['證券代號', '日期']).copy()
            db_c['大買'] = db_c['三大法人買賣超股數'] > 3000000 
            db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            active_today = db_c[db_c['日期'] == latest_db_date]
            active_codes = active_today[active_today['連買計數'] >= 1]['證券代號'].unique()
            
            res_cycle = []
            with st.status("🔄 深度分析中...") as status:
                codes = active_codes[:150].tolist()
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
                                    
                                    last_c = s_data[s_data['日期'] == latest_db_date]['連買計數'].iloc[0]
                                    buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                    sell_pt = round(curr + (avg_r * 1.6), 2)
                                    
                                    res_cycle.append({
                                        "代號": c, 
                                        "名稱": s_data['證券名稱'].iloc[0],
                                        "現價": curr, 
                                        "預期價差": round(sell_pt - curr, 2),
                                        "建議買點": buy_pt, 
                                        "預期賣點": sell_pt,
                                        "現差": round(sell_pt - curr, 2),
                                        "連買天數": int(last_c),
                                        "今日狀態": "🟢 剛發動" if last_c <= 2 else f"⚪ 連買 {int(last_c)} 天",
                                        "最佳買日": "🔥 就在今天" if last_c <= 2 else "⏳ 等待回測",
                                        "_sort": 0 if last_c <= 2 else 1,
                                        "_val": round(sell_pt - curr, 2)
                                    })
                                    break
                status.update(label="✅ 分析完成", state="complete")
            
            if res_cycle:
                df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '_val'], ascending=[True, False])
                st.dataframe(df_cycle.drop(columns=['_sort', '_val']), use_container_width=True, hide_index=True)
    else:
        st.warning("資料庫內部無任何有效數據，請點擊「自動續傳更新」。")
else:
    st.warning("請執行「自動續傳更新」以獲取歷史原始資料。")
